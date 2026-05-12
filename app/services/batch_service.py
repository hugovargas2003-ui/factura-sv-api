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
import re
import unicodedata
import uuid
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Optional

import openpyxl


# ---------------------------------------------------------------------------
# CSV/XLSX Parser with fuzzy column matching
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

ALL_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS

# Aliases: common header variations → canonical field name
_COLUMN_ALIASES: dict[str, str] = {
    # tipo_dte
    "tipo": "tipo_dte", "tipo dte": "tipo_dte", "tipo_documento": "tipo_dte",
    "tipo documento": "tipo_dte", "doc type": "tipo_dte",
    # receptor
    "tipo doc receptor": "receptor_tipo_doc", "tipo_doc": "receptor_tipo_doc",
    "tipo doc": "receptor_tipo_doc", "tipo_documento_receptor": "receptor_tipo_doc",
    "nit": "receptor_num_doc", "dui": "receptor_num_doc", "documento": "receptor_num_doc",
    "num_doc": "receptor_num_doc", "num doc": "receptor_num_doc",
    "numero documento": "receptor_num_doc", "numero_documento": "receptor_num_doc",
    "nit/dui": "receptor_num_doc", "nit_dui": "receptor_num_doc",
    "nombre": "receptor_nombre", "razon social": "receptor_nombre",
    "razon_social": "receptor_nombre", "cliente": "receptor_nombre",
    "nombre cliente": "receptor_nombre", "nombre_cliente": "receptor_nombre",
    "receptor": "receptor_nombre",
    "nrc": "receptor_nrc", "registro comercio": "receptor_nrc",
    "cod actividad": "receptor_cod_actividad", "actividad": "receptor_cod_actividad",
    "codigo actividad": "receptor_cod_actividad",
    "desc actividad": "receptor_desc_actividad",
    "descripcion actividad": "receptor_desc_actividad",
    "departamento": "receptor_departamento", "depto": "receptor_departamento",
    "municipio": "receptor_municipio",
    "direccion": "receptor_complemento", "complemento": "receptor_complemento",
    "telefono": "receptor_telefono", "tel": "receptor_telefono", "phone": "receptor_telefono",
    "correo": "receptor_correo", "email": "receptor_correo", "e-mail": "receptor_correo",
    # items
    "descripcion": "item_descripcion", "producto": "item_descripcion",
    "servicio": "item_descripcion", "detalle": "item_descripcion",
    "nombre producto": "item_descripcion", "nombre_producto": "item_descripcion",
    "precio": "item_precio", "precio unitario": "item_precio",
    "precio_unitario": "item_precio", "monto": "item_precio",
    "valor": "item_precio", "price": "item_precio",
    "cantidad": "item_cantidad", "qty": "item_cantidad", "cant": "item_cantidad",
    "unidades": "item_cantidad",
    "tipo_venta": "item_tipo_venta", "tipo venta": "item_tipo_venta",
    "exenta": "item_tipo_venta", "gravada": "item_tipo_venta",
    "afecta": "item_tipo_venta", "tipo operacion item": "item_tipo_venta",
    "tipo item": "item_tipo", "tipo_item": "item_tipo",
    "unidad medida": "item_unidad_medida", "unidad_medida": "item_unidad_medida",
    "unidad": "item_unidad_medida",
    "codigo": "item_codigo", "sku": "item_codigo", "code": "item_codigo",
    "codigo producto": "item_codigo", "codigo_producto": "item_codigo",
    # other
    "condicion": "condicion_operacion", "condicion operacion": "condicion_operacion",
    "obs": "observaciones", "notas": "observaciones", "nota": "observaciones",
}


def _normalize_header(h: str) -> str:
    """Normalize header for matching: lowercase, strip accents, collapse separators."""
    h = h.strip().lower()
    # Remove accents
    h = "".join(
        c for c in unicodedata.normalize("NFD", h) if unicodedata.category(c) != "Mn"
    )
    # Collapse separators to single space
    h = re.sub(r"[_\-./]+", " ", h).strip()
    return h


def _match_column(header: str) -> Optional[str]:
    """Match a CSV header to a canonical field name using aliases + fuzzy matching."""
    norm = _normalize_header(header)

    # Exact match on canonical name
    canon = norm.replace(" ", "_")
    if canon in ALL_COLUMNS:
        return canon

    # Alias match
    if norm in _COLUMN_ALIASES:
        return _COLUMN_ALIASES[norm]

    # Fuzzy match against aliases (threshold 0.75)
    best_score = 0.0
    best_field = None
    for alias, field in _COLUMN_ALIASES.items():
        score = SequenceMatcher(None, norm, alias).ratio()
        if score > best_score:
            best_score = score
            best_field = field
    # Also fuzzy against canonical names
    for col in ALL_COLUMNS:
        score = SequenceMatcher(None, norm, col.replace("_", " ")).ratio()
        if score > best_score:
            best_score = score
            best_field = col

    if best_score >= 0.75 and best_field:
        return best_field

    return None


def _remap_headers(raw_headers: list[str]) -> dict[str, str]:
    """Map raw CSV/XLSX headers to canonical field names. Returns {raw_header: canonical}."""
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for h in raw_headers:
        field = _match_column(h)
        if field and field not in used:
            mapping[h] = field
            used.add(field)
    return mapping


def _remap_rows(rows: list[dict], header_map: dict[str, str]) -> list[dict]:
    """Remap row keys from raw headers to canonical field names."""
    result = []
    for row in rows:
        new_row = {}
        for raw_key, value in row.items():
            canon = header_map.get(raw_key)
            if canon:
                new_row[canon] = value
            else:
                new_row[raw_key] = value
        result.append(new_row)
    return result


def parse_batch_file(content: bytes, filename: str) -> tuple[list[dict], Optional[str]]:
    """Parse CSV/XLSX into list of row dicts with fuzzy column matching. Returns (rows, error)."""
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
        # Fuzzy remap headers
        if rows:
            raw_headers = list(rows[0].keys())
            header_map = _remap_headers(raw_headers)
            if header_map:
                rows = _remap_rows(rows, header_map)
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

        raw_headers = [str(h).strip() for h in raw[0] if h]
        header_map = _remap_headers(raw_headers)
        # Build rows using canonical names where possible
        rows = []
        for r in raw[1:]:
            if not any(r):
                continue
            row = {}
            for i, h in enumerate(raw_headers):
                val = r[i] if i < len(r) else None
                canon = header_map.get(h, h.strip().lower())
                row[canon] = str(val).strip() if val is not None else ""
            rows.append(row)
        return rows, None

    return [], f"Formato no soportado: {ext}"


# ---------------------------------------------------------------------------
# Auto-sanitize row data (same normalizations as smart_import)
# ---------------------------------------------------------------------------

_TIPO_DTE_MAP = {
    "factura": "01", "consumidor final": "01", "consumidor": "01",
    "ccf": "03", "credito fiscal": "03", "comprobante credito fiscal": "03",
    "nota credito": "05", "nota de credito": "05", "nc": "05",
    "nota debito": "06", "nota de debito": "06", "nd": "06",
    "retencion": "07", "comprobante retencion": "07",
    "exportacion": "11", "factura exportacion": "11",
    "sujeto excluido": "14", "factura sujeto excluido": "14", "fse": "14",
    "donacion": "15",
}

_CONDICION_MAP = {
    "contado": "1", "efectivo": "1", "cash": "1",
    "credito": "2", "credit": "2",
    "otro": "3", "other": "3",
}


def _sanitize_batch_row(row: dict) -> tuple[dict, list[dict]]:
    """Normalize batch row data to MH format. Returns (sanitized_row, list_of_fixes)."""
    from app.services.smart_import_service import (
        _clean_nit, _clean_precio, _clean_departamento, _clean_municipio,
        _infer_tipo_item, _infer_unidad_medida,
    )
    fixes = []

    def _fix(field: str, original: str, fixed: str):
        if original != fixed:
            fixes.append({"field": field, "original": original, "fixed": fixed})

    # tipo_dte: name → code
    td = row.get("tipo_dte", "").strip()
    if td and not td.isdigit():
        new_td = _TIPO_DTE_MAP.get(td.lower(), td)
        _fix("tipo_dte", td, new_td)
        row["tipo_dte"] = new_td

    # receptor_num_doc: strip dashes/spaces
    rnd = row.get("receptor_num_doc", "").strip()
    if rnd:
        cleaned = _clean_nit(rnd)
        _fix("receptor_num_doc", rnd, cleaned)
        row["receptor_num_doc"] = cleaned

    # receptor_nrc: strip dashes
    nrc = row.get("receptor_nrc", "").strip()
    if nrc:
        cleaned = _clean_nit(nrc)
        _fix("receptor_nrc", nrc, cleaned)
        row["receptor_nrc"] = cleaned

    # receptor_departamento: name → code
    dep = row.get("receptor_departamento", "").strip()
    if dep:
        cleaned = _clean_departamento(dep)
        _fix("receptor_departamento", dep, cleaned)
        row["receptor_departamento"] = cleaned

    # receptor_municipio: name → code
    mun = row.get("receptor_municipio", "").strip()
    if mun:
        cleaned = _clean_municipio(mun)
        _fix("receptor_municipio", mun, cleaned)
        row["receptor_municipio"] = cleaned

    # item_precio: strip $, commas, European format
    precio = row.get("item_precio", "").strip()
    if precio:
        cleaned_val = _clean_precio(precio)
        cleaned_str = str(cleaned_val)
        _fix("item_precio", precio, cleaned_str)
        row["item_precio"] = cleaned_str

    # item_cantidad: strip non-numeric except dot
    cant = row.get("item_cantidad", "").strip()
    if cant:
        cleaned = re.sub(r"[^\d.]", "", cant) or cant
        _fix("item_cantidad", cant, cleaned)
        row["item_cantidad"] = cleaned

    # item_tipo: text → code
    it = row.get("item_tipo", "").strip()
    if it and not it.isdigit():
        cleaned = str(_infer_tipo_item(it))
        _fix("item_tipo", it, cleaned)
        row["item_tipo"] = cleaned

    # item_unidad_medida: text → code
    um = row.get("item_unidad_medida", "").strip()
    if um and not um.isdigit():
        cleaned = str(_infer_unidad_medida(um))
        _fix("item_unidad_medida", um, cleaned)
        row["item_unidad_medida"] = cleaned

    # condicion_operacion: text → code
    co = row.get("condicion_operacion", "").strip()
    if co and not co.isdigit():
        cleaned = _CONDICION_MAP.get(co.lower(), "1")
        _fix("condicion_operacion", co, cleaned)
        row["condicion_operacion"] = cleaned

    return row, fixes


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
        errors.append(
            f"item_precio no es número: '{item_precio_str}'. "
            "Verifique que las columnas estén en el orden correcto."
        )
        item_precio = 0

    try:
        item_cantidad = float(item_cant_str)
    except ValueError:
        errors.append(
            f"item_cantidad no es número: '{item_cant_str}'. "
            "Verifique que las columnas estén en el orden correcto."
        )
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
        "tipo_venta": row.get("item_tipo_venta", "gravada").strip() or "gravada",
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
    """Validate all rows (with auto-sanitization) and return preview with errors."""
    valid = []
    errors = []
    all_fixes = []

    for i, row in enumerate(rows, 1):
        row, fixes = _sanitize_batch_row(row)
        for f in fixes:
            all_fixes.append({"row": i, **f})
        params, err = _row_to_emit_params(row, i)
        if err:
            errors.append({"row": i, "error": err})
        else:
            valid.append({"row": i, **params})

    return {
        "total_rows": len(rows),
        "valid": len(valid),
        "invalid": len(errors),
        "auto_fixed": len(set(f["row"] for f in all_fixes)),
        "fixes": all_fixes[:50],
        "errors": errors,
        "preview": valid[:10],
    }


# ---------------------------------------------------------------------------
# Batch emit (sequential with results)
# ---------------------------------------------------------------------------

async def emit_batch(
    dte_service: Any,
    org_id: str,
    user_id: str,
    rows: list[dict],
    *,
    delivery_channels: list[str] | None = None,
) -> dict:
    """
    Emit DTEs sequentially from parsed rows.
    Returns per-row results. `delivery_channels` applies to every row in
    the batch (single global selection from the UI).
    """
    results = []
    success_count = 0
    error_count = 0

    for i, row in enumerate(rows, 1):
        row, _ = _sanitize_batch_row(row)
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
                delivery_channels=delivery_channels,
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
