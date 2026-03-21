"""
credit_alert_service.py — Automated credit balance alert notifications.

Location: app/services/credit_alert_service.py

Checks organizations with low credit balances and sends:
  - 1-20 credits: low_credits (72h cooldown)
  - 1-5 credits: critical_credits (48h cooldown)
  - 0 credits: zero_credits (24h cooldown)

Triggered via: GET /api/v1/cron/check-credits?key=<CRON_SECRET>
Dedup: subscription_email_log table prevents duplicate sends.
"""

import logging
import httpx
from datetime import datetime, timedelta
from typing import Optional

from app.services.notification_service import create_notification

logger = logging.getLogger("credit_alert_service")

# Same Google Script used by email_service.py (production-proven)
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbw5CyNlSex8xL2vJBxSjg4DOCjwzUkQgiUwgJPO1L7t9H4Z8ZCJ3glCP6chJ4Vtru6ADg/exec"

RECHARGE_URL = "https://factura-sv.algoritmos.io/dashboard/creditos"

# Alert levels: (type_key, max_balance, min_balance, cooldown_hours)
# Order matters — lowest balance first for priority selection
ALERT_LEVELS = [
    ("zero_credits", 0, 0, 24),
    ("critical_credits", 5, 1, 48),
    ("low_credits", 20, 1, 72),
]


async def check_credit_alerts(supabase) -> dict:
    """
    Main entry: scan all orgs with low credit balance and send alerts.
    Returns summary of actions taken.
    """
    now = datetime.utcnow()
    results = {"checked": 0, "sent": 0, "skipped": 0, "errors": 0, "details": []}

    # Get all active orgs with credit_balance between 0 and 20
    orgs_resp = supabase.table("organizations").select(
        "id, name, credit_balance, is_active"
    ).eq(
        "is_active", True
    ).gte(
        "credit_balance", 0
    ).lte(
        "credit_balance", 20
    ).execute()

    orgs = orgs_resp.data or []
    results["checked"] = len(orgs)
    logger.info(f"Checking {len(orgs)} organizations with low credit balance")

    for org in orgs:
        try:
            balance = org.get("credit_balance", 0)
            if balance is None:
                continue

            balance = int(balance)

            # Determine which alert level applies (lowest applicable)
            alert = _get_alert_type(balance)
            if not alert:
                results["skipped"] += 1
                continue

            notif_type, cooldown_hours = alert

            # Check if already sent this notification type recently (dedup)
            if _already_sent(supabase, org["id"], notif_type, cooldown_hours):
                results["skipped"] += 1
                continue

            # Get owner email for this org
            owner_email = _get_org_owner_email(supabase, org["id"])
            if not owner_email:
                logger.warning(f"No owner email for org {org['id']} ({org['name']})")
                results["skipped"] += 1
                continue

            # Send email notification
            success = await _send_email(
                to_email=owner_email,
                org_name=org["name"],
                balance=balance,
                notif_type=notif_type,
            )

            # Create in-app notification
            try:
                notif_title, notif_msg = _get_notification_text(notif_type, balance)
                await create_notification(
                    supabase=supabase,
                    org_id=org["id"],
                    titulo=notif_title,
                    mensaje=notif_msg,
                    tipo="warning" if notif_type == "low_credits" else "error",
                    link="/dashboard/creditos",
                )
            except Exception as e:
                logger.error(f"Failed to create in-app notification for org {org['id']}: {e}")

            # Log the attempt
            _log_notification(
                supabase=supabase,
                org_id=org["id"],
                notif_type=notif_type,
                email=owner_email,
                balance=balance,
                success=success,
            )

            if success:
                results["sent"] += 1
                results["details"].append(
                    f"✅ {notif_type} → {owner_email} ({org['name']}, balance={balance})"
                )
            else:
                results["errors"] += 1
                results["details"].append(
                    f"❌ {notif_type} → {owner_email} ({org['name']}, balance={balance})"
                )

        except Exception as e:
            logger.error(f"Error processing org {org.get('id')}: {e}")
            results["errors"] += 1

    logger.info(
        f"Credit alert run complete: {results['sent']} sent, "
        f"{results['skipped']} skipped, {results['errors']} errors"
    )
    return results


def _get_alert_type(balance: int) -> Optional[tuple]:
    """
    Determine alert type based on credit balance.
    Returns (type_key, cooldown_hours) or None.
    Priority: zero > critical > low.
    """
    if balance == 0:
        return ("zero_credits", 24)
    elif 1 <= balance <= 5:
        return ("critical_credits", 48)
    elif 6 <= balance <= 20:
        return ("low_credits", 72)
    return None


def _already_sent(supabase, org_id: str, notif_type: str, cooldown_hours: int) -> bool:
    """Check if this notification was already sent within the cooldown window."""
    cutoff = (datetime.utcnow() - timedelta(hours=cooldown_hours)).isoformat()

    try:
        result = supabase.table("subscription_email_log").select("id").eq(
            "org_id", org_id
        ).eq(
            "notification_type", notif_type
        ).gte(
            "sent_at", cutoff
        ).eq(
            "delivery_status", "sent"
        ).limit(1).execute()

        return len(result.data or []) > 0
    except Exception as e:
        logger.error(f"Dedup check failed for {org_id}: {e}")
        return False  # Send anyway if dedup fails


def _get_org_owner_email(supabase, org_id: str) -> Optional[str]:
    """Get the admin/owner email for an organization."""
    try:
        result = supabase.table("users").select("email").eq(
            "org_id", org_id
        ).eq("role", "admin").limit(1).execute()

        if result.data:
            return result.data[0].get("email")

        # Fallback: any user in the org
        result = supabase.table("users").select("email").eq(
            "org_id", org_id
        ).limit(1).execute()

        if result.data:
            return result.data[0].get("email")

        return None
    except Exception as e:
        logger.error(f"Failed to get owner email for org {org_id}: {e}")
        return None


def _log_notification(supabase, org_id, notif_type, email, balance, success):
    """Log notification attempt to subscription_email_log."""
    try:
        supabase.table("subscription_email_log").insert({
            "org_id": org_id,
            "notification_type": notif_type,
            "recipient_email": email,
            "plan": f"credits:{balance}",
            "delivery_status": "sent" if success else "failed",
        }).execute()
    except Exception as e:
        logger.error(f"Failed to log notification: {e}")


def _get_notification_text(notif_type: str, balance: int) -> tuple:
    """Return (title, message) for in-app notification."""
    if notif_type == "zero_credits":
        return (
            "Sin créditos — Emisión detenida",
            f"Su saldo de créditos es 0. La emisión de DTEs está bloqueada hasta que recargue. Recargue ahora en /dashboard/creditos.",
        )
    elif notif_type == "critical_credits":
        return (
            "Créditos casi agotados",
            f"Su saldo de créditos es {balance}. Recargue pronto para evitar interrupciones en la emisión de DTEs.",
        )
    else:
        return (
            "Créditos bajos",
            f"Su saldo de créditos es {balance}. Le recomendamos recargar para no quedarse sin créditos.",
        )


async def _send_email(
    to_email: str,
    org_name: str,
    balance: int,
    notif_type: str,
) -> bool:
    """Send the actual email via Google Apps Script."""

    subject, html = _build_email(
        org_name=org_name,
        balance=balance,
        notif_type=notif_type,
    )

    payload = {
        "to": to_email,
        "subject": subject,
        "html": html,
        "attachments": [],
    }

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.post(GOOGLE_SCRIPT_URL, json=payload)
            result = resp.json()
            if result.get("success"):
                logger.info(f"✅ {notif_type} email sent to {to_email}")
                return True
            else:
                logger.error(f"❌ Email failed: {result.get('error')}")
                return False
    except Exception as e:
        logger.error(f"❌ Email send error: {e}")
        return False


def _build_email(
    org_name: str,
    balance: int,
    notif_type: str,
) -> tuple:
    """Build subject and HTML for credit alert email. Returns (subject, html)."""

    wa_url = "https://wa.me/12672304041?text=Hola%2C%20quiero%20recargar%20cr%C3%A9ditos%20FACTURA-SV"

    if notif_type == "low_credits":
        subject = "⚠️ Créditos bajos — FACTURA-SV"
        urgency_color = "#f59e0b"
        urgency_bg = "#fffbeb"
        urgency_border = "#fcd34d"
        urgency_icon = "⚠️"
        urgency_title = "Créditos bajos"
        urgency_msg = (
            f"Su empresa <strong>{org_name}</strong> tiene actualmente "
            f"<strong>{balance} créditos</strong> disponibles. "
            f"Le recomendamos recargar pronto para evitar interrupciones "
            f"en la emisión de documentos tributarios electrónicos."
        )
    elif notif_type == "critical_credits":
        subject = "🔴 Créditos casi agotados — FACTURA-SV"
        urgency_color = "#f97316"
        urgency_bg = "#fff7ed"
        urgency_border = "#fdba74"
        urgency_icon = "🔴"
        urgency_title = "¡Créditos casi agotados!"
        urgency_msg = (
            f"<strong>Urgente:</strong> su empresa <strong>{org_name}</strong> tiene solo "
            f"<strong>{balance} créditos</strong> restantes. "
            f"Cuando llegue a 0, la emisión de DTEs se detendrá. "
            f"Recargue de inmediato para evitar interrupciones."
        )
    else:  # zero_credits
        subject = "❌ Sin créditos — Emisión detenida — FACTURA-SV"
        urgency_color = "#dc2626"
        urgency_bg = "#fef2f2"
        urgency_border = "#fca5a5"
        urgency_icon = "❌"
        urgency_title = "Sin créditos — Emisión detenida"
        urgency_msg = (
            f"Su empresa <strong>{org_name}</strong> tiene <strong>0 créditos</strong>. "
            f"La emisión de documentos tributarios electrónicos está <strong>bloqueada</strong>. "
            f"Recargue ahora para restablecer el servicio."
        )

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
        <div style="background:#4f46e5;padding:20px;border-radius:8px 8px 0 0;text-align:center">
            <h1 style="color:#fff;margin:0;font-size:20px">FACTURA-SV</h1>
            <p style="color:#c7d2fe;margin:5px 0 0;font-size:12px">Facturación Electrónica DTE</p>
        </div>

        <div style="background:#f8f9fa;padding:20px;border:1px solid #e0e0e0">
            <div style="background:{urgency_bg};border:1px solid {urgency_border};border-left:4px solid {urgency_color};padding:15px;border-radius:4px;margin-bottom:20px">
                <p style="margin:0;font-size:16px;font-weight:bold;color:{urgency_color}">
                    {urgency_icon} {urgency_title}
                </p>
                <p style="margin:8px 0 0;font-size:14px;color:#555">
                    {urgency_msg}
                </p>
            </div>

            <table style="width:100%;border-collapse:collapse;margin:15px 0">
                <tr style="background:#e8f0f8">
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Empresa</strong></td>
                    <td style="padding:8px 12px;font-size:13px;border:1px solid #d0d0d0">{org_name}</td>
                </tr>
                <tr>
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Créditos disponibles</strong></td>
                    <td style="padding:8px 12px;font-size:13px;font-weight:bold;color:{urgency_color};border:1px solid #d0d0d0">{balance}</td>
                </tr>
            </table>

            <div style="text-align:center;margin:20px 0">
                <a href="{RECHARGE_URL}" style="display:inline-block;background:#4f46e5;color:#fff;font-weight:bold;font-size:14px;padding:12px 30px;border-radius:6px;text-decoration:none">
                    Recargar Créditos →
                </a>
            </div>

            <p style="font-size:13px;color:#555;text-align:center;margin:15px 0">
                ¿Necesita ayuda para recargar?<br>
                <a href="{wa_url}" style="color:#25d366;font-weight:bold;text-decoration:none">
                    💬 Escribanos por WhatsApp
                </a>
            </p>

            <div style="background:#f0f0f0;padding:10px;border-radius:4px;margin:15px 0">
                <p style="margin:0;font-size:11px;color:#888;text-align:center">
                    <strong>Cuenta BAC para transferencia:</strong><br>
                    Cuenta: 201436482 · HUGO ERNESTO VARGAS OLIVA<br>
                    Envíe comprobante por WhatsApp para activación inmediata
                </p>
            </div>
        </div>

        <div style="background:#4f46e5;padding:12px;border-radius:0 0 8px 8px;text-align:center">
            <p style="color:#c7d2fe;margin:0;font-size:11px">
                FACTURA-SV | Efficient AI Algorithms LLC | algoritmos.io
            </p>
        </div>
    </div>
    """

    return subject, html
