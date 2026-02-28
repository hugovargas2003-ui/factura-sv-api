"""
receptor_router.py — Directorio de Receptores Frecuentes + QR de Receptor
Endpoints:
  GET    /api/v1/receptores                    — Lista receptores frecuentes (con búsqueda)
  POST   /api/v1/receptores                    — Crear/actualizar receptor frecuente
  DELETE /api/v1/receptores/{id}               — Eliminar receptor frecuente
  POST   /api/v1/receptores/qr/generate        — Generar QR encriptado (requiere auth)
  POST   /api/v1/receptores/qr/generate-public — Generar QR sin auth (página pública)
  POST   /api/v1/receptores/qr/decode          — Decodificar QR escaneado
"""

import json
import base64
import os
import logging
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from app.dependencies import get_supabase, get_current_user

logger = logging.getLogger("receptores")
router = APIRouter(prefix="/api/v1", tags=["receptores"])


# ── Models ──

class ReceptorData(BaseModel):
    tipo_documento: str = Field(..., description="'36' NIT, '13' DUI, '37' otro, '03' pasaporte")
    num_documento: str = Field(..., description="Número de documento")
    nrc: str | None = None
    nombre: str = Field(..., description="Nombre completo o razón social")
    nombre_comercial: str | None = None
    cod_actividad: str | None = None
    desc_actividad: str | None = None
    direccion_departamento: str | None = None
    direccion_municipio: str | None = None
    direccion_complemento: str | None = None
    telefono: str | None = None
    correo: str | None = None


class QRDecodeRequest(BaseModel):
    qr_data: str = Field(..., description="String leído del QR (FSV:...)")


# ── Encryption Helpers ──

def _get_encryption_key(supabase) -> bytes:
    result = supabase.table("platform_config").select("value").eq("key", "qr_encryption_key").single().execute()
    if not result.data:
        raise HTTPException(500, "QR encryption key not configured")
    return bytes.fromhex(result.data["value"])


def encrypt_receptor_data(data: dict, key: bytes) -> str:
    json_bytes = json.dumps(data, separators=(',', ':')).encode('utf-8')
    nonce = os.urandom(12)
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(json_bytes) + encryptor.finalize()
    encrypted = nonce + ciphertext + encryptor.tag
    encoded = base64.urlsafe_b64encode(encrypted).decode('ascii')
    return f"FSV:{encoded}"


def decrypt_receptor_data(qr_string: str, key: bytes) -> dict:
    if not qr_string.startswith("FSV:"):
        raise HTTPException(400, "QR no es de FACTURA-SV (prefijo FSV: no encontrado)")
    try:
        encrypted = base64.urlsafe_b64decode(qr_string[4:])
    except Exception:
        raise HTTPException(400, "QR malformado: error en decodificación Base64")
    if len(encrypted) < 28:
        raise HTTPException(400, "QR malformado: datos muy cortos")
    nonce = encrypted[:12]
    tag = encrypted[-16:]
    ciphertext = encrypted[12:-16]
    try:
        cipher = Cipher(algorithms.AES(key), modes.GCM(nonce, tag), backend=default_backend())
        decryptor = cipher.decryptor()
        json_bytes = decryptor.update(ciphertext) + decryptor.finalize()
        return json.loads(json_bytes.decode('utf-8'))
    except Exception as e:
        raise HTTPException(400, f"QR inválido: no se pudo descifrar ({e})")


def generate_qr_image(data_string: str) -> bytes:
    import qrcode
    from io import BytesIO
    qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=2)
    qr.add_data(data_string)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()


# ── Upsert Helper ──

async def _upsert_receptor(supabase, org_id: str, data: dict):
    tipo_doc = data.get("tipo_documento")
    num_doc = data.get("num_documento")
    if not tipo_doc or not num_doc:
        return None

    existing = supabase.table("receptores_frecuentes").select(
        "id, uso_count"
    ).eq("org_id", org_id).eq("tipo_documento", tipo_doc).eq("num_documento", num_doc).execute()

    now = datetime.now(timezone.utc).isoformat()

    if existing.data and len(existing.data) > 0:
        existing_id = existing.data[0]["id"]
        current_count = existing.data[0]["uso_count"]
        update_data = {**data, "org_id": org_id, "uso_count": current_count + 1, "last_used_at": now, "updated_at": now}
        result = supabase.table("receptores_frecuentes").update(update_data).eq("id", existing_id).execute()
        return {"action": "updated", "receptor": result.data[0] if result.data else None, "uso_count": current_count + 1}
    else:
        insert_data = {**data, "org_id": org_id, "uso_count": 1, "last_used_at": now, "created_at": now, "updated_at": now}
        result = supabase.table("receptores_frecuentes").insert(insert_data).execute()
        return {"action": "created", "receptor": result.data[0] if result.data else None, "uso_count": 1}



def generate_fiscal_card(nombre: str, nit: str, qr_data: str) -> bytes:
    """Genera imagen PNG de tarjeta fiscal digital con branding profesional."""
    from PIL import Image, ImageDraw, ImageFont
    import qrcode as qr_lib
    from io import BytesIO

    W, H = 900, 560
    img = Image.new("RGB", (W, H), "#FFFFFF")
    draw = ImageDraw.Draw(img)

    draw.rectangle([0, 0, W-1, H-1], outline="#1565C0", width=4)
    draw.rectangle([0, 0, W, 70], fill="#0D2137")

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
    draw.text((230, 25), "Tarjeta Fiscal Digital", fill="#90CAF9", font=font_normal)

    qr = qr_lib.QRCode(version=None, error_correction=qr_lib.constants.ERROR_CORRECT_M, box_size=8, border=1)
    qr.add_data(qr_data)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="#0D2137", back_color="white").convert("RGB")
    qr_img = qr_img.resize((280, 280))
    img.paste(qr_img, (580, 100))

    y = 100
    draw.text((30, y), "TITULAR", fill="#888888", font=font_small); y += 24
    draw.text((30, y), nombre[:40], fill="#0D2137", font=font_name); y += 36
    draw.text((30, y), "NIT / DOCUMENTO", fill="#888888", font=font_small); y += 24
    draw.text((30, y), nit, fill="#333333", font=font_normal); y += 50
    draw.line([(30, y), (540, y)], fill="#E0E0E0", width=1); y += 20
    draw.text((30, y), "Presente esta tarjeta para", fill="#555555", font=font_normal); y += 26
    draw.text((30, y), "facturacion instantanea", fill="#555555", font=font_normal); y += 40
    draw.text((30, y), "Datos encriptados AES-256. No se almacenan en servidor.", fill="#999999", font=font_small)

    draw.rectangle([0, H-40, W, H], fill="#F5F5F5")
    draw.text((20, H-30), "factura-sv.algoritmos.io/mi-tarjeta-fiscal", fill="#1565C0", font=font_small)
    draw.text((W-200, H-30), "Powered by FACTURA-SV", fill="#999999", font=font_small)

    buffer = BytesIO()
    img.save(buffer, format="PNG", quality=95)
    return buffer.getvalue()


# ── QR Endpoints ──

@router.post("/receptores/qr/generate")
async def generate_receptor_qr(
    body: ReceptorData,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Genera QR encriptado con datos fiscales del receptor. Requiere auth."""
    key = _get_encryption_key(supabase)
    data = body.dict(exclude_none=True)
    qr_string = encrypt_receptor_data(data, key)
    qr_image = generate_qr_image(qr_string)
    qr_base64 = base64.b64encode(qr_image).decode('ascii')
    return {
        "qr_data": qr_string,
        "qr_image_base64": qr_base64,
        "qr_image_data_url": f"data:image/png;base64,{qr_base64}",
        "receptor": data,
        "qr_size_chars": len(qr_string),
    }


@router.post("/receptores/qr/generate-public")
async def generate_receptor_qr_public(
    body: ReceptorData,
    supabase=Depends(get_supabase),
):
    """Genera QR sin autenticación — para /mi-tarjeta-fiscal."""
    key = _get_encryption_key(supabase)
    data = body.dict(exclude_none=True)
    qr_string = encrypt_receptor_data(data, key)
    qr_image = generate_qr_image(qr_string)
    qr_base64 = base64.b64encode(qr_image).decode('ascii')
    card_image = generate_fiscal_card(
        nombre=data.get("nombre", ""),
        nit=data.get("num_documento", ""),
        qr_data=qr_string
    )
    card_base64 = base64.b64encode(card_image).decode('ascii')

    return {
        "qr_data": qr_string,
        "qr_image_base64": qr_base64,
        "qr_image_data_url": f"data:image/png;base64,{qr_base64}",
        "card_image_base64": card_base64,
        "card_image_data_url": f"data:image/png;base64,{card_base64}",
        "qr_size_chars": len(qr_string),
    }


@router.post("/receptores/qr/decode")
async def decode_receptor_qr(
    body: QRDecodeRequest,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Decodifica QR escaneado y auto-guarda como receptor frecuente."""
    key = _get_encryption_key(supabase)
    receptor = decrypt_receptor_data(body.qr_data, key)
    org_id = user["org_id"]
    saved = False
    try:
        await _upsert_receptor(supabase, org_id, receptor)
        saved = True
    except Exception as e:
        logger.warning(f"Error auto-saving receptor from QR: {e}")
    return {"receptor": receptor, "source": "qr_scan", "saved_to_directory": saved}


# ── Directory Endpoints ──

@router.get("/receptores")
async def list_receptores(
    q: str | None = Query(None, description="Buscar por nombre, NIT, NRC"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Lista receptores frecuentes con búsqueda. Ordenados por uso."""
    org_id = user["org_id"]
    query = supabase.table("receptores_frecuentes").select("*").eq(
        "org_id", org_id
    ).order("uso_count", desc=True).order("last_used_at", desc=True).limit(limit).offset(offset)

    if q:
        query = query.or_(
            f"nombre.ilike.%{q}%,"
            f"num_documento.ilike.%{q}%,"
            f"nrc.ilike.%{q}%,"
            f"nombre_comercial.ilike.%{q}%"
        )
    result = query.execute()
    return {"receptores": result.data or [], "total": len(result.data or []), "limit": limit, "offset": offset}


@router.post("/receptores")
async def create_or_update_receptor(
    body: ReceptorData,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Crear o actualizar receptor frecuente (upsert por org + tipo_doc + num_doc)."""
    return await _upsert_receptor(supabase, user["org_id"], body.dict(exclude_none=True))


@router.delete("/receptores/{receptor_id}")
async def delete_receptor(
    receptor_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Eliminar receptor frecuente."""
    supabase.table("receptores_frecuentes").delete().eq("id", receptor_id).eq("org_id", user["org_id"]).execute()
    return {"deleted": True, "id": receptor_id}
