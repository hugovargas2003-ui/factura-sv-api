"""
webhook_service.py — Webhook system for external integrations.

Location: app/services/webhook_service.py
NEW FILE — fires webhooks on DTE events (async, non-blocking).

Events: dte.emitted, dte.invalidated, dte.rejected
"""

import hashlib
import hmac
import json
import logging
import secrets
import httpx
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger("webhook_service")

VALID_EVENTS = ["dte.emitted", "dte.invalidated", "dte.rejected", "payment.received"]


async def list_webhooks(supabase: Any, org_id: str) -> list:
    """List all webhooks for an org."""
    result = supabase.table("webhooks").select("*").eq(
        "org_id", org_id
    ).order("created_at", desc=True).execute()
    return result.data or []


async def create_webhook(
    supabase: Any, org_id: str, url: str,
    events: list[str], description: str = "",
) -> dict:
    """Create a new webhook."""
    if not url.startswith("https://"):
        raise ValueError("Webhook URL must use HTTPS")

    invalid = [e for e in events if e not in VALID_EVENTS]
    if invalid:
        raise ValueError(f"Eventos invalidos: {invalid}. Validos: {VALID_EVENTS}")

    if not events:
        raise ValueError("Debe seleccionar al menos un evento")

    secret = secrets.token_hex(32)

    record = {
        "org_id": org_id,
        "url": url,
        "events": events,
        "secret": secret,
        "active": True,
        "description": description,
        "failure_count": 0,
    }

    result = supabase.table("webhooks").insert(record).execute()
    return result.data[0] if result.data else record


async def delete_webhook(supabase: Any, org_id: str, webhook_id: str) -> dict:
    """Delete a webhook."""
    result = supabase.table("webhooks").select("id").eq(
        "id", webhook_id
    ).eq("org_id", org_id).execute()

    if not result.data:
        raise ValueError("Webhook no encontrado")

    supabase.table("webhooks").delete().eq(
        "id", webhook_id
    ).eq("org_id", org_id).execute()

    return {"success": True, "deleted": webhook_id}


async def toggle_webhook(supabase: Any, org_id: str, webhook_id: str, active: bool) -> dict:
    """Enable or disable a webhook."""
    result = supabase.table("webhooks").select("id").eq(
        "id", webhook_id
    ).eq("org_id", org_id).execute()

    if not result.data:
        raise ValueError("Webhook no encontrado")

    supabase.table("webhooks").update({"active": active}).eq(
        "id", webhook_id
    ).execute()

    return {"success": True, "webhook_id": webhook_id, "active": active}


def _sign_payload(payload: str, secret: str) -> str:
    """Generate HMAC-SHA256 signature."""
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


async def fire_webhooks(
    supabase: Any, org_id: str, event: str, data: dict
) -> None:
    """
    Fire all active webhooks for an org+event.
    Non-blocking — errors are logged, never raised.
    Called from dte_service.py post-emission hooks.
    """
    try:
        result = supabase.table("webhooks").select(
            "id, url, secret, events, failure_count"
        ).eq("org_id", org_id).eq("active", True).execute()

        hooks = result.data or []
        if not hooks:
            return

        payload = json.dumps({
            "event": event,
            "timestamp": datetime.utcnow().isoformat(),
            "data": data,
        }, default=str)

        async with httpx.AsyncClient(timeout=10.0) as client:
            for hook in hooks:
                if event not in (hook.get("events") or []):
                    continue

                signature = _sign_payload(payload, hook["secret"])

                try:
                    resp = await client.post(
                        hook["url"],
                        content=payload,
                        headers={
                            "Content-Type": "application/json",
                            "X-Webhook-Signature": signature,
                            "X-Webhook-Event": event,
                            "X-Webhook-Id": hook["id"],
                        },
                    )

                    supabase.table("webhooks").update({
                        "last_triggered_at": datetime.utcnow().isoformat(),
                        "last_status_code": resp.status_code,
                        "failure_count": 0 if resp.status_code < 400 else hook.get("failure_count", 0) + 1,
                    }).eq("id", hook["id"]).execute()

                    if resp.status_code >= 400:
                        logger.warning(f"Webhook {hook['id']} returned {resp.status_code}")

                        # Auto-disable after 10 consecutive failures
                        if (hook.get("failure_count", 0) + 1) >= 10:
                            supabase.table("webhooks").update({"active": False}).eq("id", hook["id"]).execute()
                            logger.warning(f"Webhook {hook['id']} auto-disabled after 10 failures")

                except Exception as e:
                    logger.error(f"Webhook {hook['id']} failed: {e}")
                    supabase.table("webhooks").update({
                        "last_triggered_at": datetime.utcnow().isoformat(),
                        "failure_count": hook.get("failure_count", 0) + 1,
                    }).eq("id", hook["id"]).execute()

    except Exception as e:
        logger.error(f"fire_webhooks error: {e}")
