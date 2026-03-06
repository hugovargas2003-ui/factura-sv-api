"""
smart_import_service.py — Universal Smart Import with Auto Column Mapping
=========================================================================
Handles ANY CSV/XLSX format by intelligently mapping unknown columns
to expected MH-compliant fields using fuzzy matching + heuristics.

Supports: productos, receptores, and any future import type.
All output is normalized to MH DTE schema for fiscal compliance.
"""

import csv
import io
import re
import logging
from typing import Any
from difflib import SequenceMatcher

import openpyxl

logger = logging.getLogger("smart_import")


# ═══════════════════════════════════════════════════════════
# COLUMN MAPPING DEFINITIONS — aliases for each target field
# ═══════════════════════════════════════════════════════════

FIELD_ALIASES = {
    "productos": {
        "codigo": [
            "codigo", "code", "cod", "sku", "barcode", "codigo_producto",
            "cod_producto", "product_code", "item_code", "articulo",
            "codigo_barras", "referencia", "ref", "cod producto",
            "codigo producto", "codigo de barras", "upc", "ean",
            "codigo interno", "id producto", "item_id", "product_id",
            "plu", "clave", "clave_producto", "num_articulo",
        ],
        "descripcion": [
            "descripcion", "description", "nombre", "name", "producto",
            "product", "item", "detalle", "desc", "articulo", "nombre_producto",
            "product_name", "item_name", "nombre producto",
            "nombre del producto", "concepto", "servicio", "mercaderia",
            "bien", "item_description", "product_description",
            "descripcion producto", "descripcion del producto",
            "descripcion articulo", "linea", "rubro",
        ],
        "precio_unitario": [
            "precio_unitario", "precio", "price", "unit_price", "precio_venta",
            "pvp", "monto", "valor", "costo", "cost", "precio unitario",
            "precio de venta", "precioventa", "precio venta", "importe",
            "precio sin iva", "precio con iva", "total", "amount",
            "precio_neto", "precio neto", "tarifa", "rate",
            "precio_publico", "precio publico", "sales_price",
            "sell_price", "retail_price", "precio final",
        ],
        "tipo_item": [
            "tipo_item", "tipo", "type", "item_type", "clase", "category",
            "bien_servicio", "tipo item", "tipo de item", "b/s",
            "bien o servicio", "categoria", "naturaleza", "tipo producto",
            "product_type", "item_category", "clase_item",
        ],
        "unidad_medida": [
            "unidad_medida", "unidad", "unit", "um", "uom", "medida",
            "unit_measure", "unidad medida", "unidad de medida",
            "unit_of_measure", "measure", "unid", "u/m",
        ],
        "tipo_venta": [
            "tipo_venta", "iva", "tax", "impuesto", "gravado", "exento",
            "tipo venta", "tipo de venta", "afecto", "tax_type",
            "gravada", "exenta", "tax_status", "estado_fiscal",
            "condicion_iva", "iva_status", "fiscal",
        ],
    },
    "receptores": {
        "nombre": [
            "nombre", "name", "razon_social", "razon social", "cliente",
            "customer", "empresa", "company", "denominacion", "nombre_cliente",
            "razon social", "nombre comercial", "customer_name",
            "client_name", "business_name", "contribuyente",
            "nombre contribuyente", "nombre o razon social",
            "nombre_empresa", "titular", "propietario",
        ],
        "tipo_documento": [
            "tipo_documento", "tipo documento", "tipo doc", "doc_type",
            "document_type", "tipo_doc", "td", "tipo_id",
            "tipo identificacion", "tipo de documento",
        ],
        "num_documento": [
            "num_documento", "nit", "dui", "documento", "document",
            "doc_number", "numero", "numero_documento", "num documento",
            "numero documento", "ruc", "nit/dui", "identificacion",
            "nit_dui", "numero_nit", "id_fiscal", "tax_id",
            "numero nit", "no documento", "no_documento",
        ],
        "nrc": [
            "nrc", "registro_fiscal", "registro", "fiscal_id",
            "numero_registro", "nrc contribuyente", "no_registro",
            "numero registro comercio", "registro comercio",
            "num_registro", "registro_contribuyente",
        ],
        "cod_actividad": [
            "cod_actividad", "actividad", "activity_code", "giro",
            "codigo actividad", "codigo actividad economica",
            "actividad_economica", "cod_giro", "codigo_giro",
        ],
        "desc_actividad": [
            "desc_actividad", "descripcion_actividad", "giro_descripcion",
            "activity_desc", "descripcion actividad",
            "descripcion giro", "actividad economica",
        ],
        "nombre_comercial": [
            "nombre_comercial", "nombre comercial", "trade_name",
            "razon_comercial", "dba", "marca", "brand",
        ],
        "departamento": [
            "departamento", "depto", "department", "dep", "estado",
            "provincia", "direccion_departamento", "dept",
        ],
        "municipio": [
            "municipio", "muni", "municipality", "ciudad", "city",
            "direccion_municipio", "distrito",
        ],
        "complemento": [
            "complemento", "direccion", "address", "dir", "domicilio",
            "direccion_complemento", "calle", "colonia",
            "direccion fiscal", "domicilio fiscal",
        ],
        "telefono": [
            "telefono", "phone", "tel", "celular", "mobile", "whatsapp",
            "numero_telefono", "phone_number", "contacto",
            "tel_contacto", "movil",
        ],
        "correo": [
            "correo", "email", "e-mail", "mail", "correo_electronico",
            "correo electronico", "email_address", "e_mail",
        ],
    },
}

# MH-compliant departamento codes
DEPTO_MAP = {
    "ahuachapan": "01", "santa ana": "02", "sonsonate": "03",
    "chalatenango": "04", "la libertad": "05", "san salvador": "06",
    "cuscatlan": "07", "la paz": "08", "cabanas": "09",
    "san vicente": "10", "usulutan": "11", "san miguel": "12",
    "morazan": "13", "la union": "14",
}


# ═══════════════════════════════════════════════════════════
# FUZZY MATCHING ENGINE
# ═══════════════════════════════════════════════════════════

def _normalize(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[_\-\.]", " ", s)
    s = re.sub(r"\s+", " ", s)
    for k, v in {"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","ü":"u"}.items():
        s = s.replace(k, v)
    return s


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def auto_map_columns(headers: list[str], import_type: str) -> dict:
    aliases = FIELD_ALIASES.get(import_type, {})
    if not aliases:
        return {"mapping": {}, "unmapped": headers, "confidence": {}, "preview_fields": []}

    mapping = {}
    confidence = {}
    used_targets = set()

    for header in headers:
        norm_header = _normalize(header)
        best_target = None
        best_score = 0.0

        for target_field, alias_list in aliases.items():
            if target_field in used_targets:
                continue
            for alias in alias_list:
                norm_alias = _normalize(alias)
                if norm_header == norm_alias:
                    best_target = target_field
                    best_score = 1.0
                    break
                if norm_alias in norm_header or norm_header in norm_alias:
                    score = 0.85
                    if score > best_score:
                        best_target = target_field
                        best_score = score
                score = _similarity(norm_header, norm_alias)
                if score > best_score and score >= 0.6:
                    best_target = target_field
                    best_score = score
            if best_score == 1.0:
                break

        if best_target and best_score >= 0.5:
            mapping[header] = best_target
            confidence[header] = round(best_score, 2)
            used_targets.add(best_target)

    return {
        "mapping": mapping,
        "unmapped": [h for h in headers if h not in mapping],
        "confidence": confidence,
        "preview_fields": list(aliases.keys()),
    }


# ═══════════════════════════════════════════════════════════
# VALUE INFERENCE — MH-compliant normalization
# ═══════════════════════════════════════════════════════════

def _infer_tipo_item(value: str) -> int:
    v = _normalize(value)
    if v in ("2", "servicio", "service", "s", "serv"):
        return 2
    return 1

def _infer_tipo_venta(value: str) -> str:
    v = _normalize(value)
    if any(k in v for k in ["exent", "exempt", "0%", "0.00", "no gravad"]):
        return "exenta"
    if any(k in v for k in ["no suj", "no_suj", "nosuj"]):
        return "no_sujeta"
    return "gravada"

def _infer_unidad_medida(value: str) -> int:
    v = _normalize(value)
    unit_map = {
        "unidad": 59, "pieza": 59, "pza": 59, "und": 59, "un": 59, "ea": 59,
        "servicio": 59, "service": 59, "otro": 59, "other": 59,
        "kilogramo": 23, "kg": 23, "kilo": 23,
        "litro": 24, "lt": 24, "ltr": 24,
        "metro": 25, "mts": 25,
        "libra": 26, "lb": 26,
        "galon": 27, "gal": 27,
        "docena": 22, "doc": 22, "dz": 22,
        "caja": 15, "box": 15,
        "par": 57, "pair": 57,
        "hora": 44, "hr": 44, "hour": 44,
        "dia": 45, "day": 45,
        "mes": 46, "month": 46,
        "paquete": 58, "pack": 58, "paq": 58,
        "rollo": 16, "roll": 16,
        "quintal": 28, "qq": 28,
        "arroba": 29,
    }
    for key, code in unit_map.items():
        if key in v:
            return code
    try:
        code = int(value)
        if 1 <= code <= 99:
            return code
    except (ValueError, TypeError):
        pass
    return 59

def _infer_tipo_documento(value: str) -> str:
    v = _normalize(value)
    if "dui" in v or v == "13":
        return "13"
    if "pasaporte" in v or v == "37":
        return "37"
    if "carnet" in v or "residente" in v or v == "03":
        return "03"
    return "36"

def _clean_nit(value: str) -> str:
    return re.sub(r"[^0-9]", "", value)

def _clean_precio(value: str) -> float:
    v = value.strip().replace("$", "").replace(" ", "")
    if re.match(r"^\d{1,3}(\.\d{3})*(,\d{1,2})?$", v):
        v = v.replace(".", "").replace(",", ".")
    else:
        v = v.replace(",", "")
    try:
        return round(abs(float(v)), 2)
    except (ValueError, TypeError):
        return 0.0

def _clean_departamento(value: str) -> str:
    v = _normalize(value)
    if v in DEPTO_MAP:
        return DEPTO_MAP[v]
    digits = re.sub(r"[^0-9]", "", value)
    if digits and 1 <= int(digits) <= 14:
        return digits.zfill(2)
    for name, code in DEPTO_MAP.items():
        if name in v or v in name:
            return code
    return value


def _clean_municipio(value: str, depto_code: str = "") -> str:
    """Normaliza municipio a código MH de 2 dígitos."""
    v = value.strip()
    digits = re.sub(r"[^0-9]", "", v)
    if digits and len(digits) <= 2:
        return digits.zfill(2)
    if digits and len(digits) > 2:
        return digits[:2]
    # Text value — can't map without full catalog, use safe default
    return "14"


# ═══════════════════════════════════════════════════════════
# FILE PARSER
# ═══════════════════════════════════════════════════════════

def parse_file_to_rows(content: bytes, filename: str) -> tuple[list[dict], list[str], str | None]:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == "csv":
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1")
        reader = csv.DictReader(io.StringIO(text))
        headers = [h.strip() for h in (reader.fieldnames or []) if h]
        rows = [{k.strip(): (v.strip() if v else "") for k, v in row.items() if k} for row in reader]
        return rows, headers, None
    if ext in ("xlsx", "xls"):
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            ws = wb.active
            if ws is None:
                return [], [], "Excel sin hojas activas."
            raw_rows = list(ws.iter_rows(values_only=True))
            wb.close()
        except Exception as exc:
            return [], [], f"Error al leer Excel: {exc}"
        if len(raw_rows) < 2:
            return [], [], "Archivo vacio."
        headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(raw_rows[0])]
        rows = []
        for raw in raw_rows[1:]:
            row = {}
            for i, val in enumerate(raw):
                key = headers[i] if i < len(headers) else f"col_{i}"
                row[key] = str(val).strip() if val is not None else ""
            if any(v for v in row.values()):
                rows.append(row)
        return rows, headers, None
    return [], [], f"Formato no soportado: .{ext}. Use .csv o .xlsx"


def apply_mapping(rows: list[dict], mapping: dict[str, str], import_type: str) -> list[dict]:
    mapped_rows = []
    for row in rows:
        mapped = {}
        for file_col, target_field in mapping.items():
            raw_value = row.get(file_col, "")
            if not raw_value:
                continue
            if target_field == "precio_unitario":
                mapped[target_field] = _clean_precio(raw_value)
            elif target_field == "tipo_item":
                mapped[target_field] = _infer_tipo_item(raw_value)
            elif target_field == "unidad_medida":
                mapped[target_field] = _infer_unidad_medida(raw_value)
            elif target_field == "tipo_venta":
                mapped[target_field] = _infer_tipo_venta(raw_value)
            elif target_field == "tipo_documento":
                mapped[target_field] = _infer_tipo_documento(raw_value)
            elif target_field in ("num_documento", "nrc"):
                mapped[target_field] = _clean_nit(raw_value)
            elif target_field == "departamento":
                mapped[target_field] = _clean_departamento(raw_value)
            elif target_field == "municipio":
                depto = mapped.get("departamento", "")
                mapped[target_field] = _clean_municipio(raw_value, depto)
            else:
                mapped[target_field] = raw_value
        mapped_rows.append(mapped)
    return mapped_rows


# ═══════════════════════════════════════════════════════════
# SMART IMPORT PIPELINE
# ═══════════════════════════════════════════════════════════

async def smart_import(
    content: bytes, filename: str, org_id: str,
    import_type: str, supabase: Any, custom_mapping: dict | None = None,
) -> dict:
    rows, headers, parse_error = parse_file_to_rows(content, filename)
    if parse_error:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "file", "message": parse_error}]}
    if not rows:
        return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "file", "message": "Archivo vacio."}]}

    if custom_mapping:
        mapping = {k: v for k, v in custom_mapping.items() if v and v != "__ignore__"}
    else:
        map_result = auto_map_columns(headers, import_type)
        mapping = map_result["mapping"]

    if not mapping:
        return {
            "imported": 0, "skipped": 0,
            "errors": [{"row": 0, "field": "mapping", "message": f"No se pudo mapear columnas. Encontradas: {headers}"}],
            "headers_found": headers, "mapping_used": {},
        }

    mapped_rows = apply_mapping(rows, mapping, import_type)

    if import_type == "productos":
        return await _import_productos(mapped_rows, org_id, supabase, mapping, headers)
    elif import_type == "receptores":
        return await _import_receptores(mapped_rows, org_id, supabase, mapping, headers)
    return {"imported": 0, "skipped": 0, "errors": [{"row": 0, "field": "type", "message": f"Tipo '{import_type}' no soportado."}]}


async def _import_productos(rows, org_id, supabase, mapping, headers):
    from app.services.import_service import ImportResult
    result = ImportResult()
    existing = set()
    try:
        resp = supabase.table("dte_productos").select("codigo").eq("org_id", org_id).execute()
        existing = {r["codigo"] for r in (resp.data or []) if r.get("codigo")}
    except:
        pass

    to_insert = []
    for i, row in enumerate(rows, start=2):
        desc = row.get("descripcion", "").strip()
        if not desc:
            result.add_error(i, "descripcion", "Descripcion requerida")
            continue
        precio = row.get("precio_unitario", 0)
        if isinstance(precio, str):
            precio = _clean_precio(precio)
        precio = abs(float(precio)) if precio else 0
        tipo_item = row.get("tipo_item", 1)
        if isinstance(tipo_item, str):
            tipo_item = _infer_tipo_item(tipo_item)
        if tipo_item not in (1, 2):
            tipo_item = 1
        unidad = row.get("unidad_medida", 59)
        if isinstance(unidad, str):
            unidad = _infer_unidad_medida(unidad)
        tipo_venta = row.get("tipo_venta", "gravada")
        if isinstance(tipo_venta, str):
            tipo_venta = _infer_tipo_venta(tipo_venta)
        codigo = row.get("codigo", "").strip()
        if codigo and codigo in existing:
            result.add_error(i, "codigo", f"Codigo '{codigo}' duplicado")
            continue
        to_insert.append({
            "org_id": org_id, "codigo": codigo, "descripcion": desc[:1000],
            "precio_unitario": round(float(precio), 2),
            "tipo_item": int(tipo_item), "unidad_medida": int(unidad),
            "tipo_venta": tipo_venta,
        })
        if codigo:
            existing.add(codigo)

    for start in range(0, len(to_insert), 100):
        batch = to_insert[start:start + 100]
        try:
            supabase.table("dte_productos").insert(batch).execute()
            result.imported += len(batch)
        except Exception as exc:
            for j in range(len(batch)):
                result.add_error(start + j + 2, "db", str(exc)[:120])

    res = result.to_dict()
    res["mapping_used"] = mapping
    res["headers_found"] = headers
    return res


async def _import_receptores(rows, org_id, supabase, mapping, headers):
    from app.services.import_service import ImportResult
    result = ImportResult()
    existing = set()
    try:
        resp = supabase.table("dte_receptores").select("num_documento").eq("org_id", org_id).execute()
        existing = {r["num_documento"] for r in (resp.data or []) if r.get("num_documento")}
    except:
        pass

    to_insert = []
    for i, row in enumerate(rows, start=2):
        nombre = row.get("nombre", "").strip()
        if not nombre:
            result.add_error(i, "nombre", "Nombre requerido")
            continue
        num_doc = row.get("num_documento", "").strip()
        num_doc = re.sub(r"[^0-9]", "", num_doc)
        if not num_doc:
            result.add_error(i, "num_documento", "Documento requerido")
            continue
        if num_doc in existing:
            result.add_error(i, "num_documento", f"'{num_doc}' duplicado")
            continue
        tipo_doc = row.get("tipo_documento", "36")
        if isinstance(tipo_doc, str) and not tipo_doc.isdigit():
            tipo_doc = _infer_tipo_documento(tipo_doc)
        nrc = row.get("nrc", "").strip() or None
        if nrc:
            nrc = re.sub(r"[^0-9]", "", nrc) or None
        correo = row.get("correo", "").strip() or None
        if correo and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", correo):
            correo = None
        depto = row.get("departamento", "").strip() or None
        if depto:
            depto = _clean_departamento(depto)

        to_insert.append({
            "org_id": org_id, "nombre": nombre[:200],
            "tipo_documento": tipo_doc, "num_documento": num_doc,
            "nrc": nrc,
            "cod_actividad": row.get("cod_actividad", "").strip() or None,
            "desc_actividad": row.get("desc_actividad", "").strip() or None,
            "nombre_comercial": row.get("nombre_comercial", "").strip() or None,
            "direccion_departamento": depto,
            "direccion_municipio": row.get("municipio", "").strip() or None,
            "direccion_complemento": row.get("complemento", "").strip() or None,
            "telefono": row.get("telefono", "").strip() or None,
            "correo": correo,
            "tipo_receptor": "contribuyente" if tipo_doc == "36" else "consumidor_final",
            "is_favorite": False,
        })
        existing.add(num_doc)

    for start in range(0, len(to_insert), 100):
        batch = to_insert[start:start + 100]
        try:
            supabase.table("dte_receptores").insert(batch).execute()
            result.imported += len(batch)
        except Exception as exc:
            for j in range(len(batch)):
                result.add_error(start + j + 2, "db", str(exc)[:120])

    res = result.to_dict()
    res["mapping_used"] = mapping
    res["headers_found"] = headers
    return res
