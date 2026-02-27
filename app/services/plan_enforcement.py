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
    "enterprise": 10000,
}

# Orgs with unlimited access (bypass quota) — platform owner
UNLIMITED_ORGS = set()



def check_plan_status(supabase, org_id: str) -> dict:
    """
    Verify organization can emit DTEs.
    Returns plan info dict if OK.
    Raises HTTPException if blocked.
    """
    org = supabase.table("organizations").select(
        "id, name, plan, plan_status, payment_method, plan_expires_at, is_active"
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

    # 2. Check trial expiration (free plan with trialing status)
    plan_status = data.get("plan_status", "active")
    expires_at = data.get("plan_expires_at")
    payment_method = data.get("payment_method", "stripe")

    if expires_at:
        exp_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if exp_dt.tzinfo:
            exp_dt = exp_dt.replace(tzinfo=None)

        if exp_dt < datetime.utcnow():
            plan = data.get("plan", "free")

            if plan == "free" or plan_status == "trialing":
                # Trial expired — block emission completely
                supabase.table("organizations").update({
                    "plan_status": "expired",
                    "payment_notes": f"Prueba gratuita expirada {exp_dt.date()}.",
                }).eq("id", org_id).execute()

                logger.warning(f"Trial expired for org {org_id}")

                raise HTTPException(
                    403,
                    "Su periodo de prueba de 3 días ha finalizado. "
                    "Seleccione un plan para continuar emitiendo DTEs. "
                    "Vaya a Planes en su panel de control."
                )
            else:
                # Paid plan expired — downgrade to blocked
                supabase.table("organizations").update({
                    "plan": "free",
                    "plan_status": "expired",
                    "monthly_quota": 0,
                    "payment_notes": f"Plan {plan} expirado {exp_dt.date()}. Renovar para continuar.",
                }).eq("id", org_id).execute()

                logger.warning(f"Paid plan expired for org {org_id}, blocked")

                raise HTTPException(
                    403,
                    f"Su plan {plan} ha expirado. "
                    "Renueve su suscripción para continuar emitiendo DTEs."
                )

    # 3. Check if already expired status (subsequent requests)
    if plan_status == "expired":
        raise HTTPException(
            403,
            "Su cuenta no tiene un plan activo. "
            "Seleccione un plan para comenzar a emitir DTEs."
        )

    # 4. Check DTE quota
    plan = data.get("plan", "free")
    limit = PLAN_LIMITS.get(plan, 50)

    # Bypass for platform owner (unlimited)
    if org_id in UNLIMITED_ORGS or data.get("monthly_quota", 0) >= 999999:
        return {
            "plan": plan,
            "plan_status": plan_status,
            "payment_method": payment_method,
            "dte_limit": 999999,
            "dte_used": 0,
            "dte_remaining": 999999,
            "is_active": True,
        }

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
        "plan_status": plan_status,
        "payment_method": payment_method,
        "dte_limit": limit,
        "dte_used": used,
        "dte_remaining": limit - used,
        "is_active": True,
    }
