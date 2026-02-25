"""
batch_service.py — Batch DTE emission from CSV/XLSX.

Location: app/services/batch_service.py
NEW FILE — does not modify any existing infrastructure.

Flow:
1. Parse CSV/XLSX → list of DTE requests
2. Validate all rows (preview)
3. Emit sequentially (concurrency=1 for MH rate limits)
4. Return results with per-row status
"""

import csv
import io
import uuid
from datetime import datetime
from typing import Any, Optional

import openpyxl


# ---------------------------------------------------------------------------
# CSV/XLSX Parser
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "tipo_dte", "receptor_tipo_doc", "receptor_num_doc",
    "receptor_nombre", "item_descripcion", "item_precio", "item_cantidad",
}

OPTIONAL_COLUMNS = {
    "receptor_nrc", "receptor_cod_actividad", "receptor_desc_actividad",
    "receptor_departamento", "receptor_municipio", "receptor_complemento",
    "receptor_telefono", "receptor_correo",
    "item_tipo", "item_unidad_medida", "item_codigo",
    "condicion_operacion", "observaciones",
}


def parse_batch_file(content: bytes, filename: str) -> tuple[list[dict], Optional[str]]:
    """Parse CSV/XLSX into list of row dicts. Returns (rows, error)."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "csv":
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1")
        reader = csv.DictReader(io.StringIO(text))
        rows = [
            {k.strip().lower(): (v.strip() if v else "") for k, v in row.items() if k}
            for row in reader
        ]
        return rows, None

    if ext in ("xlsx", "xls"):
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            ws = wb.active
            if not ws:
                return [], "Sin hojas activas"
            raw = list(ws.iter_rows(values_only=True))
            wb.close()
        except Exception as e:
            return [], f"Error leyendo Excel: {e}"

        if len(raw) < 2:
            return [], "Archivo sin datos"

        headers = [str(h).strip().lower() for h in raw[0] if h]
        rows = []
        for r in raw[1:]:
            if not any(r):
                continue
            row = {}
            for i, h in enumerate(headers):
                val = r[i] if i < len(r) else None
                row[h] = str(val).strip() if val is not None else ""
            rows.append(row)
        return rows, None

    return [], f"Formato no soportado: {ext}"


# ---------------------------------------------------------------------------
# Row → DTEEmitRequest converter
# ---------------------------------------------------------------------------

def _row_to_emit_params(row: dict, row_num: int) -> tuple[Optional[dict], Optional[str]]:
    """
    Convert a parsed row into params for DTEService.emit_dte().
    Returns (params_dict, error_message).
    """
    errors = []

    tipo_dte = row.get("tipo_dte", "").strip()
    if not tipo_dte:
        errors.append("tipo_dte vacío")

    receptor_num_doc = row.get("receptor_num_doc", "").strip()
    receptor_nombre = row.get("receptor_nombre", "").strip()
    if not receptor_num_doc:
        errors.append("receptor_num_doc vacío")
    if not receptor_nombre:
        errors.append("receptor_nombre vacío")

    item_desc = row.get("item_descripcion", "").strip()
    item_precio_str = row.get("item_precio", "0").strip()
    item_cant_str = row.get("item_cantidad", "1").strip()

    if not item_desc:
        errors.append("item_descripcion vacío")

    try:
        item_precio = float(item_precio_str)
    except ValueError:
        errors.append(f"item_precio inválido: {item_precio_str}")
        item_precio = 0

    try:
        item_cantidad = float(item_cant_str)
    except ValueError:
        errors.append(f"item_cantidad inválida: {item_cant_str}")
        item_cantidad = 1

    if errors:
        return None, f"Fila {row_num}: {'; '.join(errors)}"

    receptor = {
        "tipo_documento": row.get("receptor_tipo_doc", "36").strip() or "36",
        "num_documento": receptor_num_doc,
        "nombre": receptor_nombre,
        "nrc": row.get("receptor_nrc", "").strip() or None,
        "cod_actividad": row.get("receptor_cod_actividad", "").strip() or None,
        "desc_actividad": row.get("receptor_desc_actividad", "").strip() or None,
        "direccion_departamento": row.get("receptor_departamento", "06").strip() or "06",
        "direccion_municipio": row.get("receptor_municipio", "14").strip() or "14",
        "direccion_complemento": row.get("receptor_complemento", "San Salvador").strip() or "San Salvador",
        "telefono": row.get("receptor_telefono", "").strip() or None,
        "correo": row.get("receptor_correo", "").strip() or None,
    }

    item = {
        "descripcion": item_desc,
        "precio_unitario": item_precio,
        "cantidad": item_cantidad,
        "tipo_item": int(row.get("item_tipo", "2").strip() or "2"),
        "unidad_medida": int(row.get("item_unidad_medida", "59").strip() or "59"),
        "codigo": row.get("item_codigo", "").strip() or None,
        "descuento": 0,
    }

    condicion = int(row.get("condicion_operacion", "1").strip() or "1")

    return {
        "tipo_dte": tipo_dte,
        "receptor": receptor,
        "items": [item],
        "condicion_operacion": condicion,
        "observaciones": row.get("observaciones", "").strip() or None,
    }, None


# ---------------------------------------------------------------------------
# Batch preview (validate without emitting)
# ---------------------------------------------------------------------------

def preview_batch(rows: list[dict]) -> dict:
    """Validate all rows and return preview with errors."""
    valid = []
    errors = []

    for i, row in enumerate(rows, 1):
        params, err = _row_to_emit_params(row, i)
        if err:
            errors.append({"row": i, "error": err})
        else:
            valid.append({"row": i, **params})

    return {
        "total_rows": len(rows),
        "valid": len(valid),
        "invalid": len(errors),
        "errors": errors,
        "preview": valid[:10],  # First 10 for UI preview
    }


# ---------------------------------------------------------------------------
# Batch emit (sequential with results)
# ---------------------------------------------------------------------------

async def emit_batch(
    dte_service: Any,
    org_id: str,
    user_id: str,
    rows: list[dict],
) -> dict:
    """
    Emit DTEs sequentially from parsed rows.
    Returns per-row results.
    """
    results = []
    success_count = 0
    error_count = 0

    for i, row in enumerate(rows, 1):
        params, validation_err = _row_to_emit_params(row, i)

        if validation_err:
            results.append({
                "row": i, "status": "error",
                "error": validation_err, "dte_id": None,
            })
            error_count += 1
            continue

        try:
            emit_result = await dte_service.emit_dte(
                org_id=org_id,
                user_id=user_id,
                tipo_dte=params["tipo_dte"],
                receptor=params["receptor"],
                items=params["items"],
                condicion_operacion=params["condicion_operacion"],
                observaciones=params.get("observaciones"),
            )

            results.append({
                "row": i,
                "status": emit_result.get("estado", "unknown"),
                "dte_id": emit_result.get("id"),
                "numero_control": emit_result.get("numero_control"),
                "codigo_generacion": emit_result.get("codigo_generacion"),
                "sello": emit_result.get("sello_recibido"),
                "error": None,
            })
            success_count += 1

        except Exception as e:
            results.append({
                "row": i, "status": "error",
                "error": str(e), "dte_id": None,
            })
            error_count += 1

    return {
        "total": len(rows),
        "success": success_count,
        "errors": error_count,
        "results": results,
    }
