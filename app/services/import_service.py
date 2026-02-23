"""
import_service.py — Bulk CSV/XLSX import for dte_productos and dte_receptores.

Location: app/services/import_service.py
Dependencies: openpyxl (add to requirements.txt), csv (stdlib), io (stdlib)

⚠️ NEW FILE — does not modify any existing infrastructure.
"""

import csv
import io
import re
from typing import Any

# openpyxl — must be added to requirements.txt
# pip install openpyxl --break-system-packages
import openpyxl


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

class ImportResult:
    """Accumulates import stats and row-level errors."""

    def __init__(self) -> None:
        self.imported: int = 0
        self.skipped: int = 0
        self.errors: list[dict[str, Any]] = []

    def add_error(self, row: int, field: str, message: str) -> None:
        self.errors.append({"row": row, "field": field, "message": message})
        self.skipped += 1

    def to_dict(self) -> dict:
        return {
            "imported": self.imported,
            "skipped": self.skipped,
            "errors": self.errors,
        }


def _parse_file_to_rows(
    content: bytes, filename: str
) -> tuple[list[dict[str, str]], str | None]:
    """
    Parse CSV or XLSX bytes into a list of row-dicts.
    Returns (rows, error_message).  error_message is None on success.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "csv":
        try:
            text = content.decode("utf-8-sig")  # handles BOM
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
            wb = openpyxl.load_workbook(
                io.BytesIO(content), read_only=True, data_only=True
            )
            ws = wb.active
            if ws is None:
                return [], "El archivo Excel no tiene hojas activas."
            raw_rows = list(ws.iter_rows(values_only=True))
            wb.close()
        except Exception as exc:
            return [], f"Error al leer archivo Excel: {exc}"

        if len(raw_rows) < 2:
            return [], "El archivo no contiene datos (solo encabezados o vacío)."

        headers = [
            str(h).strip().lower() if h else f"col_{i}"
            for i, h in enumerate(raw_rows[0])
        ]
        rows = []
        for raw in raw_rows[1:]:
            row = {}
            for i, val in enumerate(raw):
                key = headers[i] if i < len(headers) else f"col_{i}"
                row[key] = str(val).strip() if val is not None else ""
            rows.append(row)
        return rows, None

    return [], f"Formato no soportado: .{ext}. Use .csv o .xlsx"


# ---------------------------------------------------------------------------
# Product import
# ---------------------------------------------------------------------------

_VALID_TIPO_ITEM = {1, 2}
_VALID_UNIDAD_MEDIDA = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
    39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
    57, 58, 59, 99,
}
_VALID_TIPO_VENTA = {"gravada", "exenta", "no_sujeta"}


def _validate_product_row(
    row: dict[str, str], row_num: int, result: ImportResult
) -> dict | None:
    """Validate a single product row.  Returns clean dict or None on error."""
    descripcion = row.get("descripcion", "").strip()
    if not descripcion:
        result.add_error(row_num, "descripcion", "Descripción es requerida.")
        return None
    if len(descripcion) > 1000:
        result.add_error(row_num, "descripcion", "Descripción excede 1000 caracteres.")
        return None

    # precio_unitario
    raw_precio = row.get("precio_unitario", "").strip()
    try:
        precio = round(float(raw_precio), 2)
        if precio < 0:
            raise ValueError
    except (ValueError, TypeError):
        result.add_error(
            row_num, "precio_unitario", f"Precio inválido: '{raw_precio}'. Debe ser número positivo."
        )
        return None

    # tipo_item — default 1 (Bien)
    raw_tipo = row.get("tipo_item", "1").strip()
    try:
        tipo_item = int(raw_tipo)
        if tipo_item not in _VALID_TIPO_ITEM:
            raise ValueError
    except (ValueError, TypeError):
        result.add_error(
            row_num, "tipo_item", f"Tipo ítem inválido: '{raw_tipo}'. Use 1=Bien o 2=Servicio."
        )
        return None

    # unidad_medida — default 59 (Unidad)
    raw_unidad = row.get("unidad_medida", "59").strip()
    try:
        unidad = int(raw_unidad)
        if unidad not in _VALID_UNIDAD_MEDIDA:
            raise ValueError
    except (ValueError, TypeError):
        result.add_error(
            row_num, "unidad_medida", f"Unidad medida inválida: '{raw_unidad}'. Use código MH (ej: 59=Unidad)."
        )
        return None

    # tipo_venta — default "gravada"
    tipo_venta = row.get("tipo_venta", "gravada").strip().lower()
    if tipo_venta not in _VALID_TIPO_VENTA:
        tipo_venta = "gravada"

    codigo = row.get("codigo", "").strip()

    return {
        "codigo": codigo,
        "descripcion": descripcion,
        "precio_unitario": precio,
        "tipo_item": tipo_item,
        "unidad_medida": unidad,
        "tipo_venta": tipo_venta,
    }


async def import_productos(
    content: bytes, filename: str, org_id: str, supabase_client: Any
) -> dict:
    """
    Parse and bulk-upsert products from CSV/XLSX.
    Skips duplicates by (org_id + codigo) when codigo is non-empty.
    """
    rows, parse_error = _parse_file_to_rows(content, filename)
    if parse_error:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "file", "message": parse_error}]}

    if not rows:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "file", "message": "Archivo vacío."}]}

    result = ImportResult()

    # Fetch existing codigos for this org to detect duplicates
    existing_codigos: set[str] = set()
    try:
        resp = (
            supabase_client.table("dte_productos")
            .select("codigo")
            .eq("org_id", org_id)
            .execute()
        )
        existing_codigos = {r["codigo"] for r in (resp.data or []) if r.get("codigo")}
    except Exception:
        pass  # If lookup fails, proceed without duplicate detection

    to_insert: list[dict] = []

    for i, row in enumerate(rows, start=2):  # row 2 = first data row (after header)
        validated = _validate_product_row(row, i, result)
        if validated is None:
            continue

        # Duplicate check by codigo (only if codigo is non-empty)
        if validated["codigo"] and validated["codigo"] in existing_codigos:
            result.add_error(i, "codigo", f"Código '{validated['codigo']}' ya existe. Omitido.")
            continue

        validated["org_id"] = org_id
        to_insert.append(validated)

        if validated["codigo"]:
            existing_codigos.add(validated["codigo"])

    # Bulk insert in batches of 100
    batch_size = 100
    for start in range(0, len(to_insert), batch_size):
        batch = to_insert[start : start + batch_size]
        try:
            supabase_client.table("dte_productos").insert(batch).execute()
            result.imported += len(batch)
        except Exception as exc:
            for j, item in enumerate(batch):
                result.add_error(
                    start + j + 2,
                    "db",
                    f"Error al insertar: {str(exc)[:120]}",
                )

    return result.to_dict()


# ---------------------------------------------------------------------------
# Receptor import
# ---------------------------------------------------------------------------

_NIT_PATTERN = re.compile(r"^\d{14}$")
_DUI_PATTERN = re.compile(r"^\d{9}$")
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_TIPO_DOC_MAP = {"36": _NIT_PATTERN, "13": _DUI_PATTERN}


def _validate_receptor_row(
    row: dict[str, str], row_num: int, result: ImportResult
) -> dict | None:
    """Validate a single receptor row."""
    nombre = row.get("nombre", "").strip()
    if not nombre:
        result.add_error(row_num, "nombre", "Nombre es requerido.")
        return None

    tipo_documento = row.get("tipo_documento", "36").strip()
    num_documento = row.get("num_documento", "").strip().replace("-", "")

    if not num_documento:
        result.add_error(row_num, "num_documento", "Número de documento es requerido.")
        return None

    # Validate document format
    if tipo_documento == "36" and not _NIT_PATTERN.match(num_documento):
        result.add_error(
            row_num, "num_documento", f"NIT inválido: '{num_documento}'. Debe ser 14 dígitos sin guiones."
        )
        return None
    if tipo_documento == "13" and not _DUI_PATTERN.match(num_documento):
        result.add_error(
            row_num, "num_documento", f"DUI inválido: '{num_documento}'. Debe ser 9 dígitos."
        )
        return None

    # Optional fields — empty → None (MH rule)
    nrc = row.get("nrc", "").strip() or None
    cod_actividad = row.get("cod_actividad", "").strip() or None
    desc_actividad = row.get("desc_actividad", "").strip() or None
    nombre_comercial = row.get("nombre_comercial", "").strip() or None
    departamento = row.get("departamento", "").strip() or None
    municipio = row.get("municipio", "").strip() or None
    complemento = row.get("complemento", "").strip() or None

    telefono = row.get("telefono", "").strip() or None
    if telefono and len(telefono) < 8:
        result.add_error(
            row_num, "telefono", f"Teléfono '{telefono}' debe tener mínimo 8 caracteres."
        )
        return None

    correo = row.get("correo", "").strip() or None
    if correo and not _EMAIL_PATTERN.match(correo):
        result.add_error(row_num, "correo", f"Correo inválido: '{correo}'.")
        return None

    # Determine tipo_receptor from tipo_documento
    tipo_receptor = "contribuyente" if tipo_documento == "36" else "consumidor_final"

    return {
        "tipo_documento": tipo_documento,
        "num_documento": num_documento,
        "nombre": nombre,
        "nrc": nrc,
        "cod_actividad": cod_actividad,
        "desc_actividad": desc_actividad,
        "nombre_comercial": nombre_comercial,
        "direccion_departamento": departamento,
        "direccion_municipio": municipio,
        "direccion_complemento": complemento,
        "telefono": telefono,
        "correo": correo,
        "tipo_receptor": tipo_receptor,
        "is_favorite": False,
    }


async def import_receptores(
    content: bytes, filename: str, org_id: str, supabase_client: Any
) -> dict:
    """
    Parse and bulk-insert receptors from CSV/XLSX.
    Skips duplicates by (org_id + num_documento).
    """
    rows, parse_error = _parse_file_to_rows(content, filename)
    if parse_error:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "file", "message": parse_error}]}

    if not rows:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "file", "message": "Archivo vacío."}]}

    result = ImportResult()

    # Fetch existing num_documentos for duplicate detection
    existing_docs: set[str] = set()
    try:
        resp = (
            supabase_client.table("dte_receptores")
            .select("num_documento")
            .eq("org_id", org_id)
            .execute()
        )
        existing_docs = {r["num_documento"] for r in (resp.data or []) if r.get("num_documento")}
    except Exception:
        pass

    to_insert: list[dict] = []

    for i, row in enumerate(rows, start=2):
        validated = _validate_receptor_row(row, i, result)
        if validated is None:
            continue

        if validated["num_documento"] in existing_docs:
            result.add_error(
                i, "num_documento", f"Documento '{validated['num_documento']}' ya existe. Omitido."
            )
            continue

        validated["org_id"] = org_id
        to_insert.append(validated)
        existing_docs.add(validated["num_documento"])

    # Bulk insert in batches of 100
    batch_size = 100
    for start in range(0, len(to_insert), batch_size):
        batch = to_insert[start : start + batch_size]
        try:
            supabase_client.table("dte_receptores").insert(batch).execute()
            result.imported += len(batch)
        except Exception as exc:
            for j, _ in enumerate(batch):
                result.add_error(
                    start + j + 2,
                    "db",
                    f"Error al insertar: {str(exc)[:120]}",
                )

    return result.to_dict()
