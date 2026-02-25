"""
notification_service.py — In-app notification center.

Location: app/services/notification_service.py
NEW FILE — manages notifications for quota alerts, cert expiry, payment overdue, DTE rejected.
"""

import logging
from typing import Any, Optional

logger = logging.getLogger("notification_service")

TIPOS = ["info", "warning", "error", "success"]


async def create_notification(
    supabase: Any,
    org_id: str,
    titulo: str,
    mensaje: str = "",
    tipo: str = "info",
    user_id: Optional[str] = None,
    link: Optional[str] = None,
) -> None:
    """Create a notification. Non-blocking — never raises."""
    try:
        supabase.table("notifications").insert({
            "org_id": org_id,
            "user_id": user_id,
            "tipo": tipo if tipo in TIPOS else "info",
            "titulo": titulo,
            "mensaje": mensaje,
            "leida": False,
            "link": link,
        }).execute()
    except Exception as e:
        logger.error(f"Notification create error: {e}")


async def list_notifications(
    supabase: Any,
    org_id: str,
    user_id: Optional[str] = None,
    unread_only: bool = False,
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """List notifications for org/user."""
    query = (
        supabase.table("notifications")
        .select("*", count="exact")
        .eq("org_id", org_id)
        .order("created_at", desc=True)
    )

    if user_id:
        # Show org-wide (user_id is null) + user-specific
        query = query.or_(f"user_id.is.null,user_id.eq.{user_id}")
    if unread_only:
        query = query.eq("leida", False)

    offset = (page - 1) * per_page
    query = query.range(offset, offset + per_page - 1)
    result = query.execute()

    return {
        "data": result.data or [],
        "total": result.count or 0,
        "page": page,
        "per_page": per_page,
    }


async def get_unread_count(
    supabase: Any,
    org_id: str,
    user_id: Optional[str] = None,
) -> dict:
    """Get unread notification count."""
    query = (
        supabase.table("notifications")
        .select("id", count="exact")
        .eq("org_id", org_id)
        .eq("leida", False)
    )
    if user_id:
        query = query.or_(f"user_id.is.null,user_id.eq.{user_id}")

    result = query.execute()
    return {"unread": result.count or len(result.data or [])}


async def mark_read(supabase: Any, org_id: str, notification_id: str) -> dict:
    """Mark a single notification as read."""
    supabase.table("notifications").update({"leida": True}).eq(
        "id", notification_id
    ).eq("org_id", org_id).execute()
    return {"success": True, "id": notification_id}


async def mark_all_read(
    supabase: Any,
    org_id: str,
    user_id: Optional[str] = None,
) -> dict:
    """Mark all notifications as read."""
    query = supabase.table("notifications").update({"leida": True}).eq(
        "org_id", org_id
    ).eq("leida", False)

    if user_id:
        query = query.or_(f"user_id.is.null,user_id.eq.{user_id}")

    query.execute()
    return {"success": True}


async def check_quota_alert(supabase: Any, org_id: str, used: int, limit: int) -> None:
    """Fire notification if quota exceeds 80%."""
    try:
        if limit <= 0:
            return
        pct = (used / limit) * 100
        if pct >= 80 and pct < 100:
            await create_notification(
                supabase, org_id,
                titulo="Cuota al 80%",
                mensaje=f"Ha utilizado {used} de {limit} DTEs este mes ({pct:.0f}%). Considere actualizar su plan.",
                tipo="warning",
                link="/dashboard/planes",
            )
        elif pct >= 100:
            await create_notification(
                supabase, org_id,
                titulo="Cuota agotada",
                mensaje=f"Ha alcanzado el limite de {limit} DTEs/mes. Actualice su plan para continuar emitiendo.",
                tipo="error",
                link="/dashboard/planes",
            )
    except Exception as e:
        logger.error(f"Quota alert error: {e}")


async def notify_dte_rejected(supabase: Any, org_id: str, tipo_dte: str, codigo_gen: str, error: str = "") -> None:
    """Notify when a DTE is rejected by MH."""
    try:
        await create_notification(
            supabase, org_id,
            titulo=f"DTE {tipo_dte} rechazado",
            mensaje=f"El documento {codigo_gen[:12]}... fue rechazado por MH. {error}",
            tipo="error",
            link="/dashboard/dtes",
        )
    except Exception as e:
        logger.error(f"DTE rejected notification error: {e}")
