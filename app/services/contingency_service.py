"""
contingency_service.py — Offline DTE queue when MH is down.

Location: app/services/contingency_service.py
⚠️ NEW FILE — does not modify any existing infrastructure.

Flow:
1. Client calls /dte/emit → MH returns 5xx or timeout
2. Instead of failing, DTE is queued in dte_contingency_queue
3. Background process retries queued DTEs
4. Client can check queue status via /contingency/list
"""

import json
from datetime import datetime, timezone
from typing import Any


async def queue_dte(
    supabase: Any,
    org_id: str,
    created_by: str,
    tipo_dte: str,
    receptor: dict,
    items: list,
    dte_json: dict | None = None,
    numero_control: str | None = None,
    codigo_generacion: str | None = None,
    condicion_operacion: int = 1,
    observaciones: str | None = None,
    error_message: str | None = None,
) -> dict:
    """Queue a DTE for later transmission when MH is unavailable."""
    record = {
        "org_id": org_id,
        "tipo_dte": tipo_dte,
        "receptor": json.dumps(receptor) if isinstance(receptor, dict) else receptor,
        "items": json.dumps(items) if isinstance(items, list) else items,
        "dte_json": json.dumps(dte_json) if dte_json else None,
        "numero_control": numero_control,
        "codigo_generacion": codigo_generacion,
        "condicion_operacion": condicion_operacion,
        "observaciones": observaciones,
        "status": "queued",
        "retry_count": 0,
        "error_message": error_message,
        "created_by": created_by,
    }

    result = supabase.table("dte_contingency_queue").insert(record).execute()
    row = result.data[0] if result.data else {}

    return {
        "queued": True,
        "queue_id": row.get("id"),
        "message": "DTE encolado para transmisión posterior. MH no disponible.",
        "numero_control": numero_control,
        "codigo_generacion": codigo_generacion,
    }


async def list_queue(
    supabase: Any,
    org_id: str,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List queued DTEs for an organization."""
    query = supabase.table("dte_contingency_queue").select(
        "id, tipo_dte, numero_control, codigo_generacion, status, "
        "retry_count, error_message, created_at, processed_at"
    ).eq("org_id", org_id).order("created_at", desc=True).limit(limit)

    if status:
        query = query.eq("status", status)

    result = query.execute()
    return result.data or []


async def get_queue_stats(supabase: Any, org_id: str) -> dict:
    """Get queue statistics."""
    result = supabase.table("dte_contingency_queue").select(
        "status"
    ).eq("org_id", org_id).execute()

    rows = result.data or []
    stats = {"queued": 0, "processing": 0, "completed": 0, "failed": 0, "total": len(rows)}
    for r in rows:
        s = r.get("status", "queued")
        if s in stats:
            stats[s] += 1

    return stats


async def retry_queued_dte(
    supabase: Any,
    queue_id: str,
    org_id: str,
) -> dict:
    """Mark a queued DTE for retry (resets status to 'queued')."""
    supabase.table("dte_contingency_queue").update({
        "status": "queued",
        "error_message": None,
    }).eq("id", queue_id).eq("org_id", org_id).execute()

    return {"success": True, "message": "DTE marcado para reintento"}


async def process_queue_batch(
    supabase: Any,
    org_id: str,
    dte_service: Any,
    user: dict,
    batch_size: int = 10,
) -> dict:
    """
    Process a batch of queued DTEs. Called manually or by cron.
    Returns count of processed, failed, remaining.
    """
    # Fetch queued items
    result = supabase.table("dte_contingency_queue").select("*").eq(
        "org_id", org_id
    ).eq("status", "queued").order("created_at").limit(batch_size).execute()

    items = result.data or []
    processed = 0
    failed = 0

    for item in items:
        queue_id = item["id"]

        # Mark as processing
        supabase.table("dte_contingency_queue").update({
            "status": "processing",
        }).eq("id", queue_id).execute()

        try:
            # Reconstruct DTE JSON and attempt transmission
            dte_json = item.get("dte_json")
            if isinstance(dte_json, str):
                dte_json = json.loads(dte_json)

            if not dte_json:
                raise ValueError("No hay DTE JSON para transmitir")

            # Attempt to transmit via dte_service
            # This calls the existing sign → transmit pipeline
            result_emit = await dte_service.transmit_signed_dte(
                dte_json=dte_json,
                org_id=org_id,
            )

            # Success — mark completed
            supabase.table("dte_contingency_queue").update({
                "status": "completed",
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "error_message": None,
            }).eq("id", queue_id).execute()

            processed += 1

        except Exception as e:
            retry_count = (item.get("retry_count") or 0) + 1
            new_status = "failed" if retry_count >= 5 else "queued"

            supabase.table("dte_contingency_queue").update({
                "status": new_status,
                "retry_count": retry_count,
                "error_message": str(e)[:500],
            }).eq("id", queue_id).execute()

            failed += 1

    # Count remaining
    remaining = supabase.table("dte_contingency_queue").select(
        "id", count="exact"
    ).eq("org_id", org_id).eq("status", "queued").execute()

    return {
        "processed": processed,
        "failed": failed,
        "remaining": remaining.count if hasattr(remaining, 'count') else 0,
        "message": f"Batch completado: {processed} enviados, {failed} fallidos",
    }


async def cancel_queued_dte(
    supabase: Any, queue_id: str, org_id: str
) -> dict:
    """Cancel a queued DTE (remove from queue)."""
    supabase.table("dte_contingency_queue").delete().eq(
        "id", queue_id
    ).eq("org_id", org_id).eq("status", "queued").execute()
    return {"success": True, "message": "DTE removido de la cola"}
