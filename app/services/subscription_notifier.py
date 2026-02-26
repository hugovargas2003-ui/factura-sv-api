"""
subscription_notifier.py — Automated subscription expiry email notifications.

Location: app/services/subscription_notifier.py

Checks organizations with plan_expires_at and sends:
  - 7 days before: friendly reminder
  - 3 days before: urgent reminder  
  - 1 day before: final warning
  - Expired: downgrade notice

Triggered via: GET /api/v1/cron/check-expirations?key=<CRON_SECRET>
Dedup: subscription_email_log table prevents duplicate sends.
"""

import logging
import json
import httpx
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("subscription_notifier")

# Same Google Script used by email_service.py (production-proven)
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbw5CyNlSex8xL2vJBxSjg4DOCjwzUkQgiUwgJPO1L7t9H4Z8ZCJ3glCP6chJ4Vtru6ADg/exec"

PLAN_NAMES = {
    "free": "Gratis",
    "emprendedor": "Emprendedor",
    "profesional": "Profesional",
    "contador": "Contador",
    "enterprise": "Enterprise",
}

PLAN_PRICES = {
    "emprendedor": 9.99,
    "profesional": 24.99,
    "contador": 49.99,
    "enterprise": 149.99,
}

# Notification windows: (type_key, days_before, already_expired)
NOTIFICATION_WINDOWS = [
    ("expiring_7d", 7, 6, False),
    ("expiring_3d", 3, 2, False),
    ("expiring_1d", 1, 0, False),
    ("expired", -1, -30, True),
]


async def check_and_notify(supabase) -> dict:
    """
    Main entry: scan all orgs with plan_expires_at and send notifications.
    Returns summary of actions taken.
    """
    now = datetime.utcnow()
    results = {"checked": 0, "sent": 0, "skipped": 0, "errors": 0, "details": []}

    # Get all orgs with expiration dates and active paid plans
    orgs_resp = supabase.table("organizations").select(
        "id, name, plan, payment_method, plan_expires_at, is_active"
    ).not_.is_("plan_expires_at", "null").neq(
        "plan", "free"
    ).execute()

    orgs = orgs_resp.data or []
    results["checked"] = len(orgs)
    logger.info(f"Checking {len(orgs)} organizations with expiration dates")

    for org in orgs:
        try:
            expires_str = org.get("plan_expires_at", "")
            if not expires_str:
                continue

            exp_dt = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
            if exp_dt.tzinfo:
                exp_dt = exp_dt.replace(tzinfo=None)

            days_until = (exp_dt - now).days

            # Determine which notification to send
            notif_type = _get_notification_type(days_until)
            if not notif_type:
                results["skipped"] += 1
                continue

            # Check if already sent this notification type recently (dedup)
            if _already_sent(supabase, org["id"], notif_type):
                results["skipped"] += 1
                continue

            # Get owner email for this org
            owner_email = _get_org_owner_email(supabase, org["id"])
            if not owner_email:
                logger.warning(f"No owner email for org {org['id']} ({org['name']})")
                results["skipped"] += 1
                continue

            # Send notification
            plan_name = PLAN_NAMES.get(org["plan"], org["plan"])
            success = await _send_notification(
                to_email=owner_email,
                org_name=org["name"],
                plan_name=plan_name,
                plan_id=org["plan"],
                expires_at=exp_dt,
                days_until=days_until,
                notif_type=notif_type,
            )

            # Log the attempt
            _log_notification(
                supabase=supabase,
                org_id=org["id"],
                notif_type=notif_type,
                email=owner_email,
                plan=org["plan"],
                expires_at=expires_str,
                success=success,
            )

            if success:
                results["sent"] += 1
                results["details"].append(
                    f"✅ {notif_type} → {owner_email} ({org['name']})"
                )
            else:
                results["errors"] += 1
                results["details"].append(
                    f"❌ {notif_type} → {owner_email} ({org['name']})"
                )

        except Exception as e:
            logger.error(f"Error processing org {org.get('id')}: {e}")
            results["errors"] += 1

    logger.info(
        f"Notification run complete: {results['sent']} sent, "
        f"{results['skipped']} skipped, {results['errors']} errors"
    )
    return results


def _get_notification_type(days_until: int) -> Optional[str]:
    """Determine notification type based on days until expiration."""
    if 5 <= days_until <= 7:
        return "expiring_7d"
    elif 2 <= days_until <= 4:
        return "expiring_3d"
    elif 0 <= days_until <= 1:
        return "expiring_1d"
    elif -30 <= days_until < 0:
        return "expired"
    return None


def _already_sent(supabase, org_id: str, notif_type: str) -> bool:
    """Check if this notification was already sent recently."""
    # For expiring notifications: check last 3 days
    # For expired: check last 7 days (only send once per week)
    if notif_type == "expired":
        cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    else:
        cutoff = (datetime.utcnow() - timedelta(days=3)).isoformat()

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


def _log_notification(supabase, org_id, notif_type, email, plan, expires_at, success):
    """Log notification attempt to subscription_email_log."""
    try:
        supabase.table("subscription_email_log").insert({
            "org_id": org_id,
            "notification_type": notif_type,
            "recipient_email": email,
            "plan": plan,
            "expires_at": expires_at,
            "delivery_status": "sent" if success else "failed",
        }).execute()
    except Exception as e:
        logger.error(f"Failed to log notification: {e}")


async def _send_notification(
    to_email: str,
    org_name: str,
    plan_name: str,
    plan_id: str,
    expires_at: datetime,
    days_until: int,
    notif_type: str,
) -> bool:
    """Send the actual email via Google Apps Script."""
    
    subject, html = _build_email(
        org_name=org_name,
        plan_name=plan_name,
        plan_id=plan_id,
        expires_at=expires_at,
        days_until=days_until,
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
    plan_name: str,
    plan_id: str,
    expires_at: datetime,
    days_until: int,
    notif_type: str,
) -> tuple:
    """Build subject and HTML for notification email. Returns (subject, html)."""
    
    fecha_exp = expires_at.strftime("%d/%m/%Y")
    price = PLAN_PRICES.get(plan_id, 0)
    renew_url = "https://factura-sv-production.up.railway.app/dashboard/planes"
    wa_url = "https://wa.me/12672304041?text=Hola%2C%20quiero%20renovar%20mi%20plan%20FACTURA-SV"

    if notif_type == "expiring_7d":
        subject = f"⏰ Su plan {plan_name} vence en {days_until} días — FACTURA-SV"
        urgency_color = "#f59e0b"
        urgency_bg = "#fffbeb"
        urgency_border = "#fcd34d"
        urgency_icon = "⏰"
        urgency_title = f"Su plan vence el {fecha_exp}"
        urgency_msg = (
            f"Le recordamos que su plan <strong>{plan_name}</strong> para "
            f"<strong>{org_name}</strong> vencerá en <strong>{days_until} días</strong>. "
            f"Renueve ahora para evitar interrupciones en su facturación electrónica."
        )
    elif notif_type == "expiring_3d":
        subject = f"⚠️ ¡{days_until} días! Su plan {plan_name} está por vencer — FACTURA-SV"
        urgency_color = "#f97316"
        urgency_bg = "#fff7ed"
        urgency_border = "#fdba74"
        urgency_icon = "⚠️"
        urgency_title = f"¡Quedan solo {days_until} días!"
        urgency_msg = (
            f"Su plan <strong>{plan_name}</strong> para <strong>{org_name}</strong> "
            f"vence el <strong>{fecha_exp}</strong>. Sin renovación, su cuenta será "
            f"degradada al plan Gratis (50 DTEs/mes) y perderá acceso a funciones avanzadas."
        )
    elif notif_type == "expiring_1d":
        subject = f"🚨 ÚLTIMO DÍA — Su plan {plan_name} vence mañana — FACTURA-SV"
        urgency_color = "#ef4444"
        urgency_bg = "#fef2f2"
        urgency_border = "#fca5a5"
        urgency_icon = "🚨"
        urgency_title = "¡Su plan vence mañana!"
        urgency_msg = (
            f"<strong>Última oportunidad:</strong> su plan <strong>{plan_name}</strong> "
            f"para <strong>{org_name}</strong> vence el <strong>{fecha_exp}</strong>. "
            f"Mañana su cuenta será degradada automáticamente al plan Gratis."
        )
    else:  # expired
        subject = f"❌ Su plan {plan_name} ha vencido — FACTURA-SV"
        urgency_color = "#dc2626"
        urgency_bg = "#fef2f2"
        urgency_border = "#fca5a5"
        urgency_icon = "❌"
        urgency_title = "Su plan ha vencido"
        urgency_msg = (
            f"Su plan <strong>{plan_name}</strong> para <strong>{org_name}</strong> "
            f"venció el <strong>{fecha_exp}</strong>. Su cuenta ha sido degradada al plan "
            f"Gratis (50 DTEs/mes). Renueve para recuperar todas sus funciones."
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
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Plan</strong></td>
                    <td style="padding:8px 12px;font-size:13px;border:1px solid #d0d0d0">{plan_name} (${price:.2f}/mes)</td>
                </tr>
                <tr style="background:#e8f0f8">
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Vence</strong></td>
                    <td style="padding:8px 12px;font-size:13px;font-weight:bold;color:{urgency_color};border:1px solid #d0d0d0">{fecha_exp}</td>
                </tr>
            </table>

            <div style="text-align:center;margin:20px 0">
                <a href="{renew_url}" style="display:inline-block;background:#4f46e5;color:#fff;font-weight:bold;font-size:14px;padding:12px 30px;border-radius:6px;text-decoration:none">
                    Renovar Ahora →
                </a>
            </div>

            <p style="font-size:13px;color:#555;text-align:center;margin:15px 0">
                ¿Prefiere pagar por transferencia o efectivo?<br>
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
