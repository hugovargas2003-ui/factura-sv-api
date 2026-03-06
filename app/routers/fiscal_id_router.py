"""
FACTURA-SV: Universal Fiscal Identity Standard
================================================
Estándar abierto de identificación fiscal digital para El Salvador.
Cualquier persona genera su Tarjeta Fiscal Digital gratis.
Cualquier software puede leer y verificar el QR.

Formato QR: FSV1:{base64_json}
Versión: 1.0

Especificación pública en: /api/v1/fiscal-id/spec
"""

import json
import base64
import hashlib
import re
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.dependencies import get_supabase, get_current_user

logger = logging.getLogger("fiscal_id")

router = APIRouter(prefix="/api/v1/fiscal-id", tags=["Fiscal Identity (Public Standard)"])


# ── Models ──

class FiscalIdentity(BaseModel):
    """Estructura estándar de una identidad fiscal salvadoreña."""
    v: int = Field(1, description="Versión del formato")
    tipo_doc: str = Field("36", description="Tipo documento: 36=NIT, 13=DUI, 37=Pasaporte")
    nit: str = Field(..., description="NIT sin guiones (14 dígitos) o DUI (9 dígitos)")
    nrc: Optional[str] = Field(None, description="NRC sin guiones (si contribuyente)")
    nombre: str = Field(..., description="Nombre completo o razón social")
    nombre_comercial: Optional[str] = None
    cod_actividad: Optional[str] = Field(None, description="Código CAT-019 MH")
    desc_actividad: Optional[str] = None
    depto: Optional[str] = Field(None, description="Código departamento (01-14)")
    muni: Optional[str] = Field(None, description="Código municipio (01-50)")
    dir: Optional[str] = Field(None, description="Dirección complemento")
    tel: Optional[str] = None
    email: Optional[str] = None


class GenerateRequest(BaseModel):
    """Request para generar Tarjeta Fiscal Digital."""
    tipo_documento: str = "36"
    nit: str
    nrc: Optional[str] = None
    nombre: str
    nombre_comercial: Optional[str] = None
    cod_actividad: Optional[str] = None
    desc_actividad: Optional[str] = None
    departamento: Optional[str] = None
    municipio: Optional[str] = None
    complemento: Optional[str] = None
    telefono: Optional[str] = None
    correo: Optional[str] = None
    save_to_directory: bool = Field(True, description="Guardar en directorio fiscal público")


# ── Helpers ──

def _clean_nit(value: str) -> str:
    return re.sub(r"[^0-9]", "", value)


def _encode_fiscal_qr(data: FiscalIdentity) -> str:
    """Codifica identidad fiscal a formato QR estándar abierto.

    Formato: FSV1:{base64url_json}
    NO se encripta — es estándar abierto, legible por cualquier software.
    """
    d = {k: v for k, v in data.dict().items() if v is not None and v != ""}
    json_str = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(json_str.encode("utf-8")).decode("ascii")
    return f"FSV1:{b64}"


def _decode_fiscal_qr(qr_string: str) -> dict:
    """Decodifica QR fiscal estándar.

    Soporta:
    - FSV1:{base64} — Versión 1 estándar abierto (nuevo)
    - FSV:{base64}  — Legacy formato encriptado (compatibilidad)
    """
    if qr_string.startswith("FSV1:"):
        b64 = qr_string[5:]
        json_str = base64.urlsafe_b64decode(b64).decode("utf-8")
        return json.loads(json_str)
    elif qr_string.startswith("FSV:"):
        # Legacy encrypted — cannot decode without key
        raise ValueError("QR legacy encriptado — use endpoint /api/v1/receptores/qr/decode")
    else:
        raise ValueError("Formato QR no reconocido. Esperado: FSV1: o FSV:")


def _generate_checksum(data: dict) -> str:
    canonical = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _generate_qr_png(data_string: str) -> str | None:
    """Genera QR PNG como base64. Returns None si qrcode no está instalado."""
    try:
        import qrcode
        from io import BytesIO

        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(data_string)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = BytesIO()
        img.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except ImportError:
        return None


def _generate_fiscal_card(nombre: str, nit: str, nrc: str | None, actividad: str | None, qr_data: str) -> str | None:
    """Genera tarjeta fiscal profesional como PNG base64."""
    try:
        from PIL import Image, ImageDraw, ImageFont
        import qrcode as qr_lib
        from io import BytesIO

        W, H = 900, 560
        img = Image.new("RGB", (W, H), "#FFFFFF")
        draw = ImageDraw.Draw(img)

        # Frame
        draw.rectangle([0, 0, W - 1, H - 1], outline="#4338CA", width=4)
        # Header bar
        draw.rectangle([0, 0, W, 70], fill="#1E1B4B")

        try:
            font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
            font_normal = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
            font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
            font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
        except Exception:
            font_bold = ImageFont.load_default()
            font_normal = font_bold
            font_small = font_bold
            font_name = font_bold

        draw.text((20, 18), "FACTURA-SV", fill="#FFFFFF", font=font_bold)
        draw.text((230, 25), "Tarjeta Fiscal Digital", fill="#A5B4FC", font=font_normal)

        # QR code
        qr = qr_lib.QRCode(version=None, error_correction=qr_lib.constants.ERROR_CORRECT_M, box_size=8, border=1)
        qr.add_data(qr_data)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="#1E1B4B", back_color="white").convert("RGB")
        qr_img = qr_img.resize((280, 280))
        img.paste(qr_img, (580, 100))

        # Info
        y = 100
        draw.text((30, y), "TITULAR", fill="#888888", font=font_small)
        y += 24
        draw.text((30, y), nombre[:40], fill="#1E1B4B", font=font_name)
        y += 36
        draw.text((30, y), "NIT / DOCUMENTO", fill="#888888", font=font_small)
        y += 24
        draw.text((30, y), nit, fill="#333333", font=font_normal)
        y += 30
        if nrc:
            draw.text((30, y), "NRC", fill="#888888", font=font_small)
            y += 20
            draw.text((30, y), nrc, fill="#333333", font=font_normal)
            y += 30
        if actividad:
            draw.text((30, y), "ACTIVIDAD", fill="#888888", font=font_small)
            y += 20
            draw.text((30, y), actividad[:50], fill="#333333", font=font_small)
            y += 30

        draw.line([(30, y), (540, y)], fill="#E0E0E0", width=1)
        y += 15
        draw.text((30, y), "Presente esta tarjeta para", fill="#555555", font=font_normal)
        y += 26
        draw.text((30, y), "facturacion instantanea", fill="#555555", font=font_normal)

        # Footer
        draw.rectangle([0, H - 40, W, H], fill="#F5F5F5")
        draw.text((20, H - 30), "factura-sv.algoritmos.io/mi-tarjeta-fiscal", fill="#4338CA", font=font_small)
        draw.text((W - 260, H - 30), "Estandar abierto FSV1", fill="#999999", font=font_small)

        buf = BytesIO()
        img.save(buf, format="PNG", quality=95)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except ImportError:
        return None


# ═══════════════════════════════════════════════════════════
# PUBLIC ENDPOINTS (no auth)
# ═══════════════════════════════════════════════════════════

@router.get("/spec")
async def get_specification():
    """Especificación pública del estándar de Tarjeta Fiscal Digital."""
    return {
        "standard": "FACTURA-SV Fiscal Identity Standard",
        "version": "1.0",
        "prefix": "FSV1:",
        "encoding": "base64url(JSON-UTF8)",
        "encryption": "none (open standard)",
        "country": "SV (El Salvador)",
        "authority": "Ministerio de Hacienda",
        "fields": {
            "v": {"type": "int", "required": True, "description": "Format version (always 1)"},
            "tipo_doc": {"type": "string", "required": True, "values": {"36": "NIT", "13": "DUI", "37": "Pasaporte", "03": "Carnet Residente"}},
            "nit": {"type": "string", "required": True, "description": "Tax ID, digits only (14 for NIT, 9 for DUI)"},
            "nrc": {"type": "string", "required": False, "description": "Tax registration number (contribuyentes only)"},
            "nombre": {"type": "string", "required": True, "description": "Full legal name or business name"},
            "nombre_comercial": {"type": "string", "required": False},
            "cod_actividad": {"type": "string", "required": False, "description": "Economic activity code (CAT-019 MH)"},
            "desc_actividad": {"type": "string", "required": False},
            "depto": {"type": "string", "required": False, "description": "Department code (01-14, CAT-012 MH)"},
            "muni": {"type": "string", "required": False, "description": "Municipality code (CAT-013 MH)"},
            "dir": {"type": "string", "required": False, "description": "Street address"},
            "tel": {"type": "string", "required": False},
            "email": {"type": "string", "required": False},
        },
        "how_to_decode": [
            "1. Strip prefix 'FSV1:'",
            "2. Base64url decode the remainder",
            "3. Parse as UTF-8 JSON",
            "4. Validate required fields: v, tipo_doc, nit, nombre",
        ],
        "how_to_generate": [
            "1. Build JSON object with required + optional fields",
            "2. Minify JSON (no whitespace)",
            "3. Base64url encode",
            "4. Prepend 'FSV1:'",
            "5. Generate QR code from the resulting string",
        ],
        "compatibility": "Any QR reader + base64 decoder can read this format",
        "generator": "https://factura-sv.algoritmos.io/mi-tarjeta-fiscal",
        "api_lookup": "GET /api/v1/fiscal-id/lookup?nit={nit}",
        "license": "Open standard — free to implement without license",
    }


@router.post("/generate", summary="Generar Tarjeta Fiscal Digital (sin auth)")
async def generate_fiscal_id(body: GenerateRequest, supabase=Depends(get_supabase)):
    """Genera una Tarjeta Fiscal Digital universal. Sin autenticación requerida."""
    nit_clean = _clean_nit(body.nit)
    if not nit_clean or len(nit_clean) < 9:
        raise HTTPException(400, "NIT/DUI inválido. Mínimo 9 dígitos.")
    if not body.nombre or len(body.nombre.strip()) < 3:
        raise HTTPException(400, "Nombre requerido (mínimo 3 caracteres).")

    nrc_clean = _clean_nit(body.nrc) if body.nrc else None

    fiscal = FiscalIdentity(
        v=1,
        tipo_doc=body.tipo_documento or "36",
        nit=nit_clean,
        nrc=nrc_clean,
        nombre=body.nombre.strip(),
        nombre_comercial=body.nombre_comercial or None,
        cod_actividad=body.cod_actividad or None,
        desc_actividad=body.desc_actividad or None,
        depto=body.departamento or None,
        muni=body.municipio or None,
        dir=body.complemento or None,
        tel=body.telefono or None,
        email=body.correo or None,
    )

    qr_string = _encode_fiscal_qr(fiscal)
    checksum = _generate_checksum(fiscal.dict())

    # QR image
    qr_image_b64 = _generate_qr_png(qr_string)

    # Card image
    card_image_b64 = _generate_fiscal_card(
        nombre=body.nombre.strip(),
        nit=nit_clean,
        nrc=nrc_clean,
        actividad=body.desc_actividad,
        qr_data=qr_string,
    )

    # Save to directorio_fiscal
    if body.save_to_directory:
        try:
            supabase.table("directorio_fiscal").upsert(
                {
                    "nit": nit_clean,
                    "nrc": nrc_clean,
                    "nombre": body.nombre.strip(),
                    "nombre_comercial": body.nombre_comercial or None,
                    "tipo_documento": body.tipo_documento or "36",
                    "cod_actividad": body.cod_actividad or None,
                    "desc_actividad": body.desc_actividad or None,
                    "departamento": body.departamento or None,
                    "municipio": body.municipio or None,
                    "complemento": body.complemento or None,
                    "telefono": body.telefono or None,
                    "correo": body.correo or None,
                    "qr_version": 1,
                    "last_updated_at": datetime.now(timezone.utc).isoformat(),
                    "is_active": True,
                },
                on_conflict="nit",
            ).execute()
        except Exception as e:
            logger.warning(f"Error saving to directorio_fiscal: {e}")

    return {
        "qr_string": qr_string,
        "qr_image_base64": qr_image_b64,
        "qr_image_data_url": f"data:image/png;base64,{qr_image_b64}" if qr_image_b64 else None,
        "card_image_base64": card_image_b64,
        "card_image_data_url": f"data:image/png;base64,{card_image_b64}" if card_image_b64 else None,
        "checksum": checksum,
        "format_version": 1,
        "nit": nit_clean,
        "nombre": body.nombre.strip(),
        "spec_url": "/api/v1/fiscal-id/spec",
    }


@router.post("/decode", summary="Decodificar QR Fiscal (sin auth)")
async def decode_fiscal_qr_endpoint(body: dict, supabase=Depends(get_supabase)):
    """Decodifica un QR fiscal. Soporta FSV1 (abierto) y FSV (legacy)."""
    qr_data = body.get("qr_string") or body.get("qr_data") or body.get("data", "")
    if not qr_data:
        raise HTTPException(400, "Proporcione qr_string con el contenido del QR")

    try:
        decoded = _decode_fiscal_qr(qr_data)
    except ValueError as e:
        raise HTTPException(400, str(e))

    nit = decoded.get("nit", "")
    directory_data = None
    if nit:
        try:
            result = supabase.table("directorio_fiscal").select("*").eq("nit", nit).eq("is_active", True).limit(1).execute()
            if result.data:
                directory_data = result.data[0]
        except Exception:
            pass

    return {
        "decoded": decoded,
        "from_directory": directory_data is not None,
        "directory_data": directory_data,
        "source": "directory" if directory_data else "qr_embedded",
        "format_version": decoded.get("v", 0),
        "compatible": True,
    }


@router.get("/lookup", summary="Buscar identidad fiscal por NIT (sin auth)")
async def lookup_by_nit(
    nit: str = Query(..., description="NIT a buscar (con o sin guiones)"),
    supabase=Depends(get_supabase),
):
    """Busca una identidad fiscal en el directorio público por NIT."""
    nit_clean = re.sub(r"[^0-9]", "", nit)
    if not nit_clean or len(nit_clean) < 9:
        raise HTTPException(400, "NIT inválido. Mínimo 9 dígitos.")

    result = supabase.table("directorio_fiscal").select("*").eq("nit", nit_clean).eq("is_active", True).limit(1).execute()

    if not result.data:
        return {
            "found": False,
            "nit": nit_clean,
            "message": "NIT no registrado en directorio fiscal. Genere su Tarjeta Fiscal en factura-sv.algoritmos.io/mi-tarjeta-fiscal",
        }

    data = result.data[0]
    data.pop("id", None)
    data.pop("last_updated_by", None)

    return {"found": True, "nit": nit_clean, "identity": data, "qr_version": data.get("qr_version", 1)}


@router.get("/search", summary="Buscar en directorio fiscal (sin auth)")
async def search_directory(
    q: str = Query(..., min_length=3, description="Búsqueda por nombre o NIT"),
    limit: int = Query(10, le=50),
    supabase=Depends(get_supabase),
):
    """Busca identidades fiscales por nombre o NIT parcial."""
    digits = re.sub(r"[^0-9]", "", q)
    if len(digits) >= 4:
        result = (
            supabase.table("directorio_fiscal")
            .select("nit, nombre, nombre_comercial, cod_actividad, desc_actividad")
            .ilike("nit", f"%{digits}%")
            .eq("is_active", True)
            .limit(limit)
            .execute()
        )
    else:
        result = (
            supabase.table("directorio_fiscal")
            .select("nit, nombre, nombre_comercial, cod_actividad, desc_actividad")
            .ilike("nombre", f"%{q}%")
            .eq("is_active", True)
            .limit(limit)
            .execute()
        )

    return {"results": result.data or [], "total": len(result.data or []), "query": q}


# ═══════════════════════════════════════════════════════════
# AUTHENTICATED ENDPOINT — for DTE emission flow
# ═══════════════════════════════════════════════════════════

@router.post("/resolve-for-dte", summary="Resolver receptor para emisión de DTE")
async def resolve_for_dte(
    body: dict,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Resuelve datos del receptor desde QR o NIT para llenar formulario de DTE.

    Prioridad: 1) QR → 2) directorio_fiscal → 3) receptores_frecuentes de la org
    """
    qr_string = body.get("qr_string")
    nit = body.get("nit")
    org_id = user.get("org_id")

    receptor = None
    source = None

    # 1. From QR
    if qr_string:
        try:
            decoded = _decode_fiscal_qr(qr_string)
            nit = decoded.get("nit")
            receptor = {
                "tipo_documento": decoded.get("tipo_doc", "36"),
                "num_documento": decoded.get("nit", ""),
                "nrc": decoded.get("nrc", ""),
                "nombre": decoded.get("nombre", ""),
                "nombre_comercial": decoded.get("nombre_comercial", ""),
                "cod_actividad": decoded.get("cod_actividad", ""),
                "desc_actividad": decoded.get("desc_actividad", ""),
                "direccion_departamento": decoded.get("depto", ""),
                "direccion_municipio": decoded.get("muni", ""),
                "direccion_complemento": decoded.get("dir", ""),
                "telefono": decoded.get("tel", ""),
                "correo": decoded.get("email", ""),
            }
            source = "qr"
        except Exception:
            pass

    # 2. Enrich from directorio_fiscal
    if nit:
        nit_clean = _clean_nit(nit)
        try:
            dir_result = (
                supabase.table("directorio_fiscal")
                .select("*")
                .eq("nit", nit_clean)
                .eq("is_active", True)
                .limit(1)
                .execute()
            )
            if dir_result.data:
                d = dir_result.data[0]
                receptor = {
                    "tipo_documento": d.get("tipo_documento", "36"),
                    "num_documento": d.get("nit", ""),
                    "nrc": d.get("nrc", ""),
                    "nombre": d.get("nombre", ""),
                    "nombre_comercial": d.get("nombre_comercial", ""),
                    "cod_actividad": d.get("cod_actividad", ""),
                    "desc_actividad": d.get("desc_actividad", ""),
                    "direccion_departamento": d.get("departamento", ""),
                    "direccion_municipio": d.get("municipio", ""),
                    "direccion_complemento": d.get("complemento", ""),
                    "telefono": d.get("telefono", ""),
                    "correo": d.get("correo", ""),
                }
                source = "directory"
        except Exception:
            pass

    # 3. Fallback: org's receptores_frecuentes
    if nit and org_id and not receptor:
        nit_clean = _clean_nit(nit)
        try:
            freq_result = (
                supabase.table("receptores_frecuentes")
                .select("*")
                .eq("org_id", org_id)
                .eq("num_documento", nit_clean)
                .limit(1)
                .execute()
            )
            if freq_result.data:
                receptor = freq_result.data[0]
                source = "org_catalog"
        except Exception:
            pass

    if not receptor:
        return {"found": False, "message": "Receptor no encontrado. Ingrese datos manualmente."}

    return {"found": True, "source": source, "receptor": receptor}
