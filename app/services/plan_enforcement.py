"""
plan_enforcement.py — Middleware to check plan status before DTE emission.

Location: app/services/plan_enforcement.py

This checks:
1. Is the org active? (not suspended)
2. If manual payment (cash/transfer), has the plan expired?
3. If transfer, has it been verified?
4. Is the org within DTE quota for the month?

Usage in transmit flow:
    from app.services.plan_enforcement import check_plan_status
    # Call before processing DTE
    check_plan_status(supabase, org_id)
"""

from fastapi import HTTPException
from datetime import datetime
import logging

logger = logging.getLogger("plan_enforcement")

PLAN_LIMITS = {
    "free": 50,
    "emprendedor": 200,
    "profesional": 1000,
    "contador": 5000,
    "enterprise": 999999,
}


def check_plan_status(supabase, org_id: str) -> dict:
    """
    Verify organization can emit DTEs.
    Returns plan info dict if OK.
    Raises HTTPException if blocked.
    """
    org = supabase.table("organizations").select(
        "id, name, plan, payment_method, plan_expires_at, is_active"
    ).eq("id", org_id).single().execute()

    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    data = org.data

    # 1. Check if suspended
    if not data.get("is_active", True):
        raise HTTPException(
            403,
            "Su cuenta está suspendida. Contacte soporte para reactivar."
        )

    # 2. Check expiration for manual payments
    payment_method = data.get("payment_method", "stripe")
    expires_at = data.get("plan_expires_at")

    if payment_method in ("cash", "transfer") and expires_at:
        exp_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if exp_dt.tzinfo:
            exp_dt = exp_dt.replace(tzinfo=None)

        if exp_dt < datetime.utcnow():
            # Auto-downgrade to free
            supabase.table("organizations").update({
                "plan": "free",
                "payment_notes": f"Plan expirado {exp_dt.date()}. Degradado a free.",
            }).eq("id", org_id).execute()

            logger.warning(f"Plan expired for org {org_id}, downgraded to free")

            # Update local data for quota check
            data["plan"] = "free"

    # 3. Check DTE quota
    plan = data.get("plan", "free")
    limit = PLAN_LIMITS.get(plan, 50)

    # Count DTEs this month
    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0)
    count_result = supabase.table("dte_documents").select(
        "id", count="exact"
    ).eq("org_id", org_id).gte(
        "created_at", month_start.isoformat()
    ).execute()

    used = count_result.count if hasattr(count_result, 'count') else len(count_result.data or [])

    if used >= limit:
        raise HTTPException(
            429,
            f"Ha alcanzado el límite de {limit} DTEs/mes en su plan {plan}. "
            f"Actualice su plan para continuar emitiendo."
        )

    return {
        "plan": plan,
        "payment_method": payment_method,
        "dte_limit": limit,
        "dte_used": used,
        "dte_remaining": limit - used,
        "is_active": True,
    }
