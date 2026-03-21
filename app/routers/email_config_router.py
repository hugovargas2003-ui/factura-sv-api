"""
FACTURA-SV: Email Config Router
================================
Per-org SMTP configuration for custom email sending.
Orgs can use their own SMTP server (Gmail, Outlook, custom domain)
or fall back to the platform's default GAS email.
"""

import smtplib
import logging
from email.mime.text import MIMEText

from fastapi import APIRouter, Depends, HTTPException, Body

from app.dependencies import get_current_user, get_supabase
from app.services.encryption_service import EncryptionService
from supabase import Client as SupabaseClient

logger = logging.getLogger("factura-sv.email-config")

router = APIRouter(prefix="/config/email", tags=["Email Config"])

_enc: EncryptionService | None = None


def _get_enc() -> EncryptionService:
    global _enc
    if _enc is None:
        _enc = EncryptionService()
    return _enc


@router.get("")
async def get_email_config(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Read email config for current org."""
    org_id = user["org_id"]
    result = db.table("org_email_config").select("*").eq("org_id", org_id).limit(1).execute()
    if not result.data:
        return {"use_custom_email": False, "configured": False}
    config = result.data[0]
    has_password = bool(config.get("smtp_password_encrypted"))
    config.pop("smtp_password_encrypted", None)
    config["has_password"] = has_password
    return config


@router.put("")
async def update_email_config(
    body: dict = Body(...),
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Save/update SMTP config for current org."""
    org_id = user["org_id"]

    data = {
        "org_id": org_id,
        "use_custom_email": body.get("use_custom_email", False),
        "smtp_host": (body.get("smtp_host") or "").strip() or None,
        "smtp_port": body.get("smtp_port", 587),
        "smtp_user": (body.get("smtp_user") or "").strip() or None,
        "from_name": (body.get("from_name") or "").strip() or None,
        "from_email": (body.get("from_email") or "").strip() or None,
        "use_tls": body.get("use_tls", True),
        "is_verified": False,
        "updated_at": "now()",
    }

    if body.get("smtp_password"):
        data["smtp_password_encrypted"] = _get_enc().encrypt_string(
            body["smtp_password"], org_id
        ).decode("utf-8")

    result = db.table("org_email_config").upsert(data, on_conflict="org_id").execute()
    return {"status": "ok", "message": "Configuración guardada. Envíe un email de prueba para verificar."}


@router.post("/test")
async def test_email_config(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Send a test email using the org's SMTP config."""
    org_id = user["org_id"]
    result = db.table("org_email_config").select("*").eq("org_id", org_id).limit(1).execute()
    if not result.data or not result.data[0].get("smtp_host"):
        raise HTTPException(400, "No hay configuración SMTP. Configure primero.")

    cfg = result.data[0]
    password = _get_enc().decrypt_string(
        cfg["smtp_password_encrypted"].encode("utf-8"), org_id
    )

    try:
        msg = MIMEText(
            f"Este es un email de prueba de FACTURA-SV.\n\n"
            f"Si recibe este mensaje, su configuración SMTP está correcta.\n"
            f"Los DTEs que emita se enviarán desde: {cfg.get('from_email') or cfg['smtp_user']}\n\n"
            f"— FACTURA-SV",
            "plain", "utf-8"
        )
        msg["Subject"] = "FACTURA-SV — Prueba de correo exitosa"
        msg["From"] = f"{cfg.get('from_name') or 'FACTURA-SV'} <{cfg.get('from_email') or cfg['smtp_user']}>"
        msg["To"] = cfg["smtp_user"]

        server = smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=10)
        if cfg.get("use_tls", True):
            server.starttls()
        server.login(cfg["smtp_user"], password)
        server.send_message(msg)
        server.quit()

        db.table("org_email_config").update({
            "is_verified": True,
            "last_test_at": "now()",
            "last_test_result": "ok",
        }).eq("org_id", org_id).execute()

        return {"status": "ok", "message": f"Email de prueba enviado a {cfg['smtp_user']}"}

    except Exception as e:
        db.table("org_email_config").update({
            "is_verified": False,
            "last_test_at": "now()",
            "last_test_result": str(e)[:200],
        }).eq("org_id", org_id).execute()

        err = str(e).lower()
        if "authentication" in err or "credentials" in err:
            detail = "Contraseña incorrecta. Si usa Gmail, necesita una 'Contraseña de Aplicación' (no su contraseña normal)."
        elif "connection" in err or "timeout" in err:
            detail = f"No se pudo conectar a {cfg['smtp_host']}:{cfg['smtp_port']}. Verifique servidor y puerto."
        elif "tls" in err or "ssl" in err:
            detail = "Error de seguridad TLS. Intente desactivar TLS o cambiar el puerto a 465."
        else:
            detail = f"Error: {str(e)[:150]}"

        raise HTTPException(400, detail)


@router.delete("")
async def delete_email_config(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Delete custom email config (revert to platform email)."""
    org_id = user["org_id"]
    db.table("org_email_config").delete().eq("org_id", org_id).execute()
    return {"status": "ok", "message": "Configuración eliminada. Se usará el correo de la plataforma."}
