"""
audit_service.py — Complete audit trail of user actions.

Location: app/services/audit_service.py
NEW FILE — logs all significant actions across the platform.
"""

import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger("audit_service")


async def log_action(
    supabase: Any,
    org_id: str,
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
    action: str = "",
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    details: Optional[dict] = None,
    ip_address: Optional[str] = None,
) -> None:
    """Log an action to the audit trail. Non-blocking — never raises."""
    try:
        supabase.table("audit_log").insert({
            "org_id": org_id,
            "user_id": user_id,
            "user_email": user_email,
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "details": details or {},
            "ip_address": ip_address,
        }).execute()
    except Exception as e:
        logger.error(f"Audit log error: {e}")


async def list_logs(
    supabase: Any,
    org_id: str,
    action: Optional[str] = None,
    entity_type: Optional[str] = None,
    user_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
) -> dict:
    """List audit logs with filters."""
    query = (
        supabase.table("audit_log")
        .select("*", count="exact")
        .eq("org_id", org_id)
        .order("created_at", desc=True)
    )

    if action:
        query = query.eq("action", action)
    if entity_type:
        query = query.eq("entity_type", entity_type)
    if user_id:
        query = query.eq("user_id", user_id)
    if date_from:
        query = query.gte("created_at", date_from)
    if date_to:
        query = query.lte("created_at", date_to + "T23:59:59")

    offset = (page - 1) * per_page
    query = query.range(offset, offset + per_page - 1)
    result = query.execute()

    return {
        "data": result.data or [],
        "total": result.count or 0,
        "page": page,
        "per_page": per_page,
    }


async def get_audit_summary(supabase: Any, org_id: str) -> dict:
    """Quick summary stats for audit dashboard."""
    result = supabase.table("audit_log").select(
        "action, entity_type, created_at"
    ).eq("org_id", org_id).order("created_at", desc=True).limit(500).execute()

    rows = result.data or []
    actions = {}
    entities = {}
    recent_users = set()

    for r in rows:
        a = r.get("action", "unknown")
        actions[a] = actions.get(a, 0) + 1
        et = r.get("entity_type", "other")
        entities[et] = entities.get(et, 0) + 1

    return {
        "total_events": len(rows),
        "by_action": actions,
        "by_entity": entities,
    }
