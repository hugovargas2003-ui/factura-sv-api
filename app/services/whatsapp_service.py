"""
whatsapp_service.py — WhatsApp Cloud API integration for DTE PDF delivery.

Location: app/services/whatsapp_service.py
NEW FILE — does not modify any existing infrastructure.

Requires WhatsApp Business API credentials (Meta Business account):
- phone_number_id: from Meta Business dashboard
- access_token: permanent or long-lived token
- waba_id: WhatsApp Business Account ID

Credentials stored per-org in dte_credentials table (whatsapp_* columns).
"""

import logging
import httpx
from typing import Any, Optional
import base64

logger = logging.getLogger(__name__)

WHATSAPP_API_BASE = "https://graph.facebook.com/v21.0"


async def send_dte_pdf(
    phone_number_id: str,
    access_token: str,
    recipient_phone: str,
    pdf_bytes: bytes,
    filename: str,
    caption: str = "",
) -> dict:
    """
    Send a DTE PDF via WhatsApp Cloud API.
    Flow: 1) Upload media  2) Send document message.
    """
    if not all([phone_number_id, access_token, recipient_phone, pdf_bytes]):
        raise ValueError("Faltan credenciales de WhatsApp o datos del documento")

    # Normalize phone: remove spaces, dashes, ensure country code
    phone = _normalize_phone(recipient_phone)

    async with httpx.AsyncClient(timeout=30) as client:
        # Step 1: Upload PDF as media
        upload_url = f"{WHATSAPP_API_BASE}/{phone_number_id}/media"
        files = {
            "file": (filename, pdf_bytes, "application/pdf"),
        }
        data = {
            "messaging_product": "whatsapp",
            "type": "document",
        }
        headers = {"Authorization": f"Bearer {access_token}"}

        upload_resp = await client.post(upload_url, files=files, data=data, headers=headers)
        if upload_resp.status_code != 200:
            logger.error(f"WhatsApp media upload failed: {upload_resp.text}")
            raise ValueError(f"Error subiendo PDF a WhatsApp: {upload_resp.status_code}")

        media_id = upload_resp.json().get("id")
        if not media_id:
            raise ValueError("No se obtuvo media_id de WhatsApp")

        # Step 2: Send document message
        send_url = f"{WHATSAPP_API_BASE}/{phone_number_id}/messages"
        message_payload = {
            "messaging_product": "whatsapp",
            "to": phone,
            "type": "document",
            "document": {
                "id": media_id,
                "filename": filename,
                "caption": caption,
            },
        }

        send_resp = await client.post(send_url, json=message_payload, headers=headers)
        if send_resp.status_code not in (200, 201):
            logger.error(f"WhatsApp send failed: {send_resp.text}")
            raise ValueError(f"Error enviando mensaje WhatsApp: {send_resp.status_code}")

        result = send_resp.json()
        message_id = result.get("messages", [{}])[0].get("id", "")
        logger.info(f"WhatsApp PDF sent to {phone}: message_id={message_id}")

        return {
            "success": True,
            "message_id": message_id,
            "phone": phone,
        }


async def get_whatsapp_config(supabase: Any, org_id: str) -> dict:
    """Get WhatsApp configuration for an org."""
    result = supabase.table("dte_credentials").select(
        "whatsapp_phone_number_id, whatsapp_waba_id, whatsapp_enabled"
    ).eq("org_id", org_id).single().execute()

    if not result.data:
        return {"configured": False, "enabled": False}

    return {
        "configured": bool(result.data.get("whatsapp_phone_number_id")),
        "enabled": bool(result.data.get("whatsapp_enabled")),
        "phone_number_id": result.data.get("whatsapp_phone_number_id", ""),
        "waba_id": result.data.get("whatsapp_waba_id", ""),
    }


async def save_whatsapp_config(
    supabase: Any, org_id: str, encryption_service: Any, data: dict,
) -> dict:
    """Save WhatsApp configuration for an org."""
    update = {
        "whatsapp_phone_number_id": data.get("phone_number_id", ""),
        "whatsapp_waba_id": data.get("waba_id", ""),
        "whatsapp_enabled": data.get("enabled", False),
    }

    # Encrypt access token if provided
    if data.get("access_token"):
        encrypted = encryption_service.encrypt_string(data["access_token"], org_id)
        update["whatsapp_access_token_encrypted"] = encrypted

    supabase.table("dte_credentials").update(update).eq("org_id", org_id).execute()

    return {"success": True, "message": "Configuración WhatsApp guardada"}


def _normalize_phone(phone: str) -> str:
    """Normalize phone number for WhatsApp API (E.164 format)."""
    phone = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone.startswith("+"):
        phone = phone[1:]
    # If starts with 503 (El Salvador), keep as is
    if phone.startswith("503"):
        return phone
    # If 8 digits, assume El Salvador
    if len(phone) == 8:
        return "503" + phone
    return phone
