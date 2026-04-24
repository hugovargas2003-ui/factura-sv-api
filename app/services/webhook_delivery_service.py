"""
FACTURA-SV: Webhook Delivery Service with Retry Queue
=======================================================
Persistent delivery log with exponential backoff retry.
Replaces fire-and-forget with reliable delivery.

Table: webhook_deliveries
  - Tracks each delivery attempt
  - Exponential backoff: 5s, 30s, 5min, 30min, 1h
  - Dead letter after 5 failed attempts
"""
import hashlib
import hmac
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger("webhook_delivery")

RETRY_DELAYS = [5, 30, 300, 1800, 3600]  # seconds: 5s, 30s, 5min, 30min, 1h


def _sign_payload(payload: str, secret: str) -> str:
    """Generate HMAC-SHA256 signature."""
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


async def create_delivery(
    supabase: Any, webhook_id: str, org_id: str,
    url: str, secret: str, event: str, data: dict,
) -> str:
    """Create a new webhook delivery record and attempt first delivery."""
    payload = json.dumps({
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }, default=str)

    record = {
        "webhook_id": webhook_id,
        "org_id": org_id,
        "event_type": event,
        "payload": json.loads(payload),
        "url": url,
        "secret": secret,
        "status": "pending",
        "attempts": 0,
        "next_retry_at": datetime.now(timezone.utc).isoformat(),
    }

    result = supabase.table("webhook_deliveries").insert(record).execute()
    delivery_id = result.data[0]["id"] if result.data else None

    if delivery_id:
        await attempt_delivery(supabase, delivery_id)

    return delivery_id


async def attempt_delivery(supabase: Any, delivery_id: str) -> bool:
    """Attempt to deliver a webhook. Returns True if successful."""
    result = supabase.table("webhook_deliveries").select("*").eq(
        "id", delivery_id
    ).single().execute()

    if not result.data:
        return False

    delivery = result.data
    payload_str = json.dumps(delivery["payload"], default=str)
    signature = _sign_payload(payload_str, delivery["secret"])

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                delivery["url"],
                content=payload_str,
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-Signature": signature,
                    "X-Webhook-Event": delivery["event_type"],
                    "X-Webhook-Delivery": delivery_id,
                },
            )

        attempts = delivery["attempts"] + 1

        if resp.status_code < 300:
            # Success
            supabase.table("webhook_deliveries").update({
                "status": "delivered",
                "attempts": attempts,
                "last_attempt_at": datetime.now(timezone.utc).isoformat(),
                "last_status_code": resp.status_code,
                "delivered_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", delivery_id).execute()

            # Reset failure_count on webhook
            supabase.table("webhooks").update({
                "last_triggered_at": datetime.now(timezone.utc).isoformat(),
                "last_status_code": resp.status_code,
                "failure_count": 0,
            }).eq("id", delivery["webhook_id"]).execute()

            logger.info(f"Webhook delivered: {delivery_id} -> {resp.status_code}")
            return True
        else:
            _schedule_retry(supabase, delivery_id, delivery, attempts, resp.status_code, f"HTTP {resp.status_code}")
            return False

    except Exception as e:
        attempts = delivery["attempts"] + 1
        _schedule_retry(supabase, delivery_id, delivery, attempts, None, str(e))
        return False


def _schedule_retry(
    supabase: Any, delivery_id: str, delivery: dict,
    attempts: int, status_code: int | None, error: str,
):
    """Schedule next retry or mark as dead letter."""
    if attempts >= len(RETRY_DELAYS):
        # Dead letter — no more retries
        supabase.table("webhook_deliveries").update({
            "status": "dead",
            "attempts": attempts,
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
            "last_status_code": status_code,
            "last_error": error,
        }).eq("id", delivery_id).execute()

        # Increment failure count on webhook
        supabase.table("webhooks").update({
            "last_triggered_at": datetime.now(timezone.utc).isoformat(),
            "last_status_code": status_code,
            "failure_count": (delivery.get("attempts", 0) or 0) + 1,
        }).eq("id", delivery["webhook_id"]).execute()

        logger.warning(f"Webhook delivery dead: {delivery_id} after {attempts} attempts")
        return

    delay = RETRY_DELAYS[attempts]
    next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)

    supabase.table("webhook_deliveries").update({
        "status": "pending",
        "attempts": attempts,
        "last_attempt_at": datetime.now(timezone.utc).isoformat(),
        "last_status_code": status_code,
        "last_error": error,
        "next_retry_at": next_retry.isoformat(),
    }).eq("id", delivery_id).execute()

    logger.info(f"Webhook retry scheduled: {delivery_id} attempt {attempts + 1}/{len(RETRY_DELAYS)} in {delay}s")


async def process_retry_queue(supabase: Any) -> int:
    """Process webhook deliveries due for retry. Returns count processed."""
    now = datetime.now(timezone.utc).isoformat()

    pending = supabase.table("webhook_deliveries").select("id").eq(
        "status", "pending"
    ).lte("next_retry_at", now).limit(50).execute()

    count = 0
    for delivery in (pending.data or []):
        await attempt_delivery(supabase, delivery["id"])
        count += 1

    if count > 0:
        logger.info(f"Processed {count} webhook retries")
    return count


async def list_deliveries(
    supabase: Any, org_id: str, limit: int = 50, webhook_id: str = None,
) -> list:
    """List recent webhook deliveries for an org."""
    query = supabase.table("webhook_deliveries").select(
        "id, webhook_id, event_type, status, attempts, "
        "last_status_code, last_error, delivered_at, created_at, url"
    ).eq("org_id", org_id).order("created_at", desc=True).limit(limit)

    if webhook_id:
        query = query.eq("webhook_id", webhook_id)

    result = query.execute()
    return result.data or []


async def retry_delivery(supabase: Any, org_id: str, delivery_id: str) -> dict:
    """Manually retry a dead delivery."""
    result = supabase.table("webhook_deliveries").select("*").eq(
        "id", delivery_id
    ).eq("org_id", org_id).single().execute()

    if not result.data:
        raise ValueError("Delivery no encontrada")

    if result.data["status"] not in ("dead", "pending"):
        raise ValueError("Solo se pueden reintentar deliveries fallidas")

    # Reset to pending with immediate retry
    supabase.table("webhook_deliveries").update({
        "status": "pending",
        "attempts": 0,
        "next_retry_at": datetime.now(timezone.utc).isoformat(),
        "last_error": None,
    }).eq("id", delivery_id).execute()

    success = await attempt_delivery(supabase, delivery_id)
    return {"success": success, "delivery_id": delivery_id}
