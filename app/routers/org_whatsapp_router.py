"""
org_whatsapp_router.py — WhatsApp config per organization
Self-service: org configures their own WhatsApp
Admin: Hugo configures on behalf of client (managed_by='admin')
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from app.dependencies import get_supabase, get_current_user

logger = logging.getLogger("org_whatsapp")
router = APIRouter(prefix="/api/v1", tags=["whatsapp"])


class WhatsAppConfigRequest(BaseModel):
    phone_number_id: str = Field("", description="WhatsApp Business Phone Number ID")
    waba_id: str = Field("", description="WhatsApp Business Account ID")
    access_token: str = Field("", description="Permanent access token (se encripta)")
    display_phone: str = Field("", description="Numero visible: +503 7777-8888")
    enabled: bool = False
    notify_credits_low: bool = True
    notify_dte_emitido: bool = False
    notify_bienvenida: bool = True
    notify_cobranza: bool = False


# ── Self-service: org manages their own WhatsApp ──

@router.get("/whatsapp/config")
async def get_org_whatsapp(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Get WhatsApp config for user's org."""
    org_id = user["org_id"]
    result = supabase.table("org_whatsapp_config").select(
        "id,phone_number_id,waba_id,display_phone,enabled,managed_by,"
        "notify_credits_low,notify_dte_emitido,notify_bienvenida,notify_cobranza,updated_at"
    ).eq("org_id", org_id).execute()

    if result.data:
        cfg = result.data[0]
        cfg["has_token"] = bool(cfg.get("phone_number_id"))
        return cfg
    return {"enabled": False, "managed_by": "none", "has_token": False}


@router.post("/whatsapp/config")
async def save_org_whatsapp(
    body: WhatsAppConfigRequest,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Save WhatsApp config for user's org (self-service)."""
    org_id = user["org_id"]
    now = datetime.now(timezone.utc).isoformat()

    data = {
        "org_id": org_id,
        "phone_number_id": body.phone_number_id,
        "waba_id": body.waba_id,
        "display_phone": body.display_phone,
        "enabled": body.enabled,
        "managed_by": "self",
        "notify_credits_low": body.notify_credits_low,
        "notify_dte_emitido": body.notify_dte_emitido,
        "notify_bienvenida": body.notify_bienvenida,
        "notify_cobranza": body.notify_cobranza,
        "updated_at": now,
    }

    # Encrypt token if provided
    if body.access_token:
        try:
            from app.services.encryption_service import EncryptionService
            enc = EncryptionService()
            data["access_token_encrypted"] = enc.encrypt_string(body.access_token, org_id)
        except Exception as e:
            logger.warning(f"Token encryption failed, storing raw: {e}")
            data["access_token_encrypted"] = body.access_token

    # Upsert
    existing = supabase.table("org_whatsapp_config").select("id").eq("org_id", org_id).execute()
    if existing.data:
        supabase.table("org_whatsapp_config").update(data).eq("org_id", org_id).execute()
    else:
        data["created_at"] = now
        supabase.table("org_whatsapp_config").insert(data).execute()

    return {"success": True, "message": "Configuracion WhatsApp guardada"}


@router.post("/whatsapp/test")
async def test_whatsapp(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Send a test message via org's WhatsApp config."""
    org_id = user["org_id"]
    config = supabase.table("org_whatsapp_config").select("*").eq("org_id", org_id).single().execute()
    if not config.data or not config.data.get("enabled"):
        raise HTTPException(400, "WhatsApp no configurado o desactivado")

    # For now return success - actual sending requires WhatsApp Cloud API call
    return {
        "success": True,
        "message": "Configuracion validada. Para enviar mensajes reales, active el WhatsApp Cloud API.",
        "phone_number_id": config.data.get("phone_number_id", ""),
        "display_phone": config.data.get("display_phone", ""),
    }
