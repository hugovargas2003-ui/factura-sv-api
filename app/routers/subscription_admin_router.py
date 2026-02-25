"""
subscription_admin_router.py — Manual subscription management for FACTURA-SV.

Location: app/routers/subscription_admin_router.py

Provides:
  ADMIN endpoints (require super-admin auth):
    GET  /api/v1/admin/subscriptions         — List all orgs with subscription status
    POST /api/v1/admin/subscriptions/override — Manually set plan (cash/transfer)
    POST /api/v1/admin/subscriptions/extend   — Extend existing plan
    POST /api/v1/admin/subscriptions/suspend  — Suspend/reactivate org
    GET  /api/v1/admin/subscriptions/expiring — List expiring soon
    GET  /api/v1/admin/payments               — List all manual payments
    POST /api/v1/admin/payments/{id}/verify   — Verify a transfer payment
    POST /api/v1/admin/payments/{id}/reject   — Reject a transfer payment

  CLIENT endpoint (authenticated user):
    POST /api/v1/billing/transfer-claim       — Client reports bank transfer
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger("subscription_admin")

router = APIRouter()

# ---------------------------------------------------------------------------
# Config: Super-admin email(s) that can manage subscriptions
# ---------------------------------------------------------------------------
SUPER_ADMINS = {"hugovargas2003@msn.com"}

# Plan limits for reference
PLAN_CONFIG = {
    "free":         {"dte_limit": 50,    "price_monthly": 0},
    "basico":       {"dte_limit": 200,   "price_monthly": 9.99},
    "profesional":  {"dte_limit": 1000,  "price_monthly": 49.99},
    "enterprise":   {"dte_limit": 999999, "price_monthly": 149.99},
}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SubscriptionOverride(BaseModel):
    """Admin manually sets a subscription (cash or transfer payment)."""
    org_id: str
    plan_tier: str = Field(..., pattern="^(basico|profesional|enterprise)$")
    payment_method: str = Field(..., pattern="^(cash|transfer)$")
    months: int = Field(..., ge=1, le=36)
    amount: float = Field(..., gt=0)
    # Cash fields
    cash_receipt: Optional[str] = None
    # Transfer fields
    transfer_ref: Optional[str] = None
    transfer_bank: Optional[str] = None
    transfer_date: Optional[str] = None  # YYYY-MM-DD
    # Notes
    admin_notes: Optional[str] = None


class SubscriptionExtend(BaseModel):
    """Extend an existing manual subscription."""
    org_id: str
    months: int = Field(..., ge=1, le=36)
    amount: float = Field(..., gt=0)
    payment_method: str = Field(..., pattern="^(cash|transfer)$")
    cash_receipt: Optional[str] = None
    transfer_ref: Optional[str] = None
    admin_notes: Optional[str] = None


class SuspendRequest(BaseModel):
    """Suspend or reactivate an organization."""
    org_id: str
    suspend: bool = True  # True=suspend, False=reactivate
    reason: Optional[str] = None


class TransferClaim(BaseModel):
    """Client self-reports a bank transfer."""
    transfer_ref: str = Field(..., min_length=3, max_length=100,
                              description="Código de verificación del banco")
    transfer_amount: float = Field(..., gt=0,
                                   description="Monto transferido")
    transfer_bank: str = Field(..., min_length=2, max_length=100,
                               description="Banco de origen")
    transfer_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$",
                               description="Fecha de transferencia YYYY-MM-DD")
    plan_tier: str = Field(..., pattern="^(basico|profesional|enterprise)$")
    months: int = Field(1, ge=1, le=12)
    client_notes: Optional[str] = None


class PaymentAction(BaseModel):
    """Verify or reject a payment."""
    admin_notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_supabase():
    """Import supabase client."""
    from app.dependencies import get_supabase
    return get_supabase()


def _require_super_admin(user: dict):
    """Check if user is a super admin."""
    email = user.get("email", "")
    if email not in SUPER_ADMINS:
        raise HTTPException(403, "Acceso denegado: se requiere super-admin")


async def _get_current_user_from_request(request):
    """Get current user from JWT."""
    from app.dependencies import get_current_user
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(401, "Se requiere Authorization header")

    token = auth_header.replace("Bearer ", "")
    supabase = _get_supabase()

    try:
        user_response = supabase.auth.get_user(token)
        u = user_response.user
        if not u:
            raise HTTPException(401, "Token inválido")
    except Exception:
        raise HTTPException(401, "Token inválido o expirado")

    result = supabase.table("users").select(
        "org_id, role, email, full_name"
    ).eq("id", u.id).single().execute()

    if not result.data:
        raise HTTPException(403, "Usuario sin organización")

    return {
        "user_id": str(u.id),
        "org_id": result.data["org_id"],
        "email": result.data.get("email", ""),
        "role": result.data.get("role", "member"),
        "full_name": result.data.get("full_name", ""),
    }


# ===================================================================
# ADMIN ENDPOINTS
# ===================================================================

@router.get("/api/v1/admin/subscriptions")
async def list_subscriptions(request: "fastapi.Request"):
    """List all organizations with subscription status."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    # Use the admin view
    try:
        result = supabase.table("admin_subscription_overview").select("*").execute()
        return {"data": result.data or [], "total": len(result.data or [])}
    except Exception:
        # Fallback if view doesn't exist yet
        result = supabase.table("organizations").select(
            "id, name, nit, plan, payment_method, plan_expires_at, "
            "plan_started_at, plan_months, is_active, payment_notes, created_at"
        ).order("created_at", desc=True).execute()

        orgs = result.data or []
        for org in orgs:
            expires = org.get("plan_expires_at")
            if expires:
                exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                now = datetime.now(exp_dt.tzinfo) if exp_dt.tzinfo else datetime.utcnow()
                org["days_remaining"] = max(0, (exp_dt - now).days)
                if exp_dt < now:
                    org["subscription_status"] = "VENCIDO"
                elif (exp_dt - now).days < 7:
                    org["subscription_status"] = "Por vencer"
                else:
                    org["subscription_status"] = "Activo"
            else:
                org["days_remaining"] = None
                pm = org.get("payment_method", "stripe")
                org["subscription_status"] = "Free" if pm == "free" else "Stripe Auto"

        return {"data": orgs, "total": len(orgs)}


@router.post("/api/v1/admin/subscriptions/override")
async def override_subscription(body: SubscriptionOverride, request: "fastapi.Request"):
    """
    Admin manually sets a plan for an organization.
    Used for: cash payments, verified transfers, special deals.
    """
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    # Verify org exists
    org = supabase.table("organizations").select("id, name, plan").eq(
        "id", body.org_id
    ).single().execute()
    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    # Calculate expiration
    now = datetime.utcnow()
    expires_at = now + timedelta(days=body.months * 30)

    # Update organization
    supabase.table("organizations").update({
        "plan": body.plan_tier,
        "payment_method": body.payment_method,
        "plan_expires_at": expires_at.isoformat(),
        "plan_started_at": now.isoformat(),
        "plan_months": body.months,
        "is_active": True,
        "payment_notes": body.admin_notes or f"{body.payment_method} ${body.amount} x{body.months}m",
    }).eq("id", body.org_id).execute()

    # Create payment record
    payment_data = {
        "org_id": body.org_id,
        "payment_method": body.payment_method,
        "amount": body.amount,
        "plan": body.plan_tier,
        "months": body.months,
        "status": "active",
        "verified_by": user["email"],
        "verified_at": now.isoformat(),
        "period_start": now.isoformat(),
        "period_end": expires_at.isoformat(),
        "admin_notes": body.admin_notes,
    }

    if body.payment_method == "cash":
        payment_data["cash_receipt"] = body.cash_receipt
        payment_data["transfer_verified"] = True
    elif body.payment_method == "transfer":
        payment_data["transfer_ref"] = body.transfer_ref
        payment_data["transfer_bank"] = body.transfer_bank
        payment_data["transfer_date"] = body.transfer_date
        payment_data["transfer_verified"] = True
        payment_data["transfer_amount"] = body.amount

    supabase.table("manual_payments").insert(payment_data).execute()

    logger.info(
        f"Subscription override: {org.data['name']} → {body.plan_tier} "
        f"({body.payment_method}, ${body.amount}, {body.months}m) by {user['email']}"
    )

    return {
        "success": True,
        "message": f"Plan {body.plan_tier} activado para {org.data['name']}",
        "org_id": body.org_id,
        "plan": body.plan_tier,
        "payment_method": body.payment_method,
        "expires_at": expires_at.isoformat(),
        "months": body.months,
        "amount": body.amount,
    }


@router.post("/api/v1/admin/subscriptions/extend")
async def extend_subscription(body: SubscriptionExtend, request: "fastapi.Request"):
    """Extend an existing subscription by N months."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    org = supabase.table("organizations").select(
        "id, name, plan_tier, plan_expires_at"
    ).eq("id", body.org_id).single().execute()

    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    # Calculate new expiration from current expiration (or now if expired)
    current_expires = org.data.get("plan_expires_at")
    if current_expires:
        base = datetime.fromisoformat(current_expires.replace("Z", "+00:00"))
        if base.tzinfo:
            base = base.replace(tzinfo=None)
        if base < datetime.utcnow():
            base = datetime.utcnow()
    else:
        base = datetime.utcnow()

    new_expires = base + timedelta(days=body.months * 30)

    supabase.table("organizations").update({
        "plan_expires_at": new_expires.isoformat(),
        "is_active": True,
        "payment_notes": body.admin_notes or f"Extensión +{body.months}m ${body.amount}",
    }).eq("id", body.org_id).execute()

    # Log payment
    supabase.table("manual_payments").insert({
        "org_id": body.org_id,
        "payment_method": body.payment_method,
        "amount": body.amount,
        "plan_tier": org.data["plan"],
        "months": body.months,
        "status": "active",
        "verified_by": user["email"],
        "verified_at": datetime.utcnow().isoformat(),
        "period_start": base.isoformat(),
        "period_end": new_expires.isoformat(),
        "admin_notes": body.admin_notes or f"Extensión +{body.months}m",
        "transfer_verified": True,
        "cash_receipt": body.cash_receipt,
        "transfer_ref": body.transfer_ref,
    }).execute()

    logger.info(
        f"Subscription extended: {org.data['name']} +{body.months}m → {new_expires.date()}"
    )

    return {
        "success": True,
        "message": f"Plan extendido {body.months} meses para {org.data['name']}",
        "new_expires_at": new_expires.isoformat(),
    }


@router.post("/api/v1/admin/subscriptions/suspend")
async def suspend_org(body: SuspendRequest, request: "fastapi.Request"):
    """Suspend or reactivate an organization."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    org = supabase.table("organizations").select("id, name").eq(
        "id", body.org_id
    ).single().execute()

    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    action = "Suspendido" if body.suspend else "Reactivado"

    supabase.table("organizations").update({
        "is_active": not body.suspend,
        "payment_notes": f"{action}: {body.reason or 'Sin razón especificada'}",
    }).eq("id", body.org_id).execute()

    logger.info(f"Org {action.lower()}: {org.data['name']} by {user['email']}")

    return {
        "success": True,
        "message": f"{org.data['name']} {action.lower()}",
        "is_active": not body.suspend,
    }


@router.get("/api/v1/admin/subscriptions/expiring")
async def list_expiring(request: "fastapi.Request"):
    """List subscriptions expiring in the next 30 days."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()
    cutoff = (datetime.utcnow() + timedelta(days=30)).isoformat()

    result = supabase.table("organizations").select(
        "id, name, nit, plan, payment_method, plan_expires_at, is_active"
    ).not_.is_("plan_expires_at", "null").lte(
        "plan_expires_at", cutoff
    ).eq("is_active", True).order("plan_expires_at").execute()

    orgs = result.data or []
    for org in orgs:
        exp = datetime.fromisoformat(org["plan_expires_at"].replace("Z", "+00:00"))
        if exp.tzinfo:
            exp = exp.replace(tzinfo=None)
        org["days_remaining"] = max(0, (exp - datetime.utcnow()).days)
        org["is_expired"] = exp < datetime.utcnow()

    return {"data": orgs, "total": len(orgs)}


@router.get("/api/v1/admin/payments")
async def list_payments(request: "fastapi.Request"):
    """List all manual payments with filters."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    # Get query params
    status_filter = request.query_params.get("status")
    method_filter = request.query_params.get("method")

    query = supabase.table("manual_payments").select(
        "*, organizations(name, nit)"
    ).order("created_at", desc=True).limit(100)

    if status_filter:
        query = query.eq("status", status_filter)
    if method_filter:
        query = query.eq("payment_method", method_filter)

    result = query.execute()

    return {"data": result.data or [], "total": len(result.data or [])}


@router.post("/api/v1/admin/payments/{payment_id}/verify")
async def verify_payment(payment_id: str, body: PaymentAction, request: "fastapi.Request"):
    """Admin verifies a bank transfer payment → activates plan."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    payment = supabase.table("manual_payments").select("*").eq(
        "id", payment_id
    ).single().execute()

    if not payment.data:
        raise HTTPException(404, "Pago no encontrado")

    if payment.data["status"] not in ("pending",):
        raise HTTPException(400, f"Pago ya está en estado: {payment.data['status']}")

    now = datetime.utcnow()

    # Update payment
    supabase.table("manual_payments").update({
        "status": "active",
        "transfer_verified": True,
        "verified_by": user["email"],
        "verified_at": now.isoformat(),
        "admin_notes": body.admin_notes,
        "updated_at": now.isoformat(),
    }).eq("id", payment_id).execute()

    # Activate org plan
    supabase.table("organizations").update({
        "plan": payment.data["plan_tier"],
        "payment_method": "transfer",
        "plan_expires_at": payment.data["period_end"],
        "plan_started_at": now.isoformat(),
        "plan_months": payment.data["months"],
        "is_active": True,
        "payment_notes": f"Transfer verificada: {payment.data.get('transfer_ref', 'N/A')}",
    }).eq("id", payment.data["org_id"]).execute()

    logger.info(
        f"Payment verified: {payment_id} → org {payment.data['org_id']} "
        f"({payment.data['plan_tier']}, {payment.data['months']}m)"
    )

    return {
        "success": True,
        "message": "Pago verificado y plan activado",
        "payment_id": payment_id,
        "org_id": payment.data["org_id"],
    }


@router.post("/api/v1/admin/payments/{payment_id}/reject")
async def reject_payment(payment_id: str, body: PaymentAction, request: "fastapi.Request"):
    """Admin rejects a transfer claim → downgrades org to free."""
    from fastapi import Request
    user = await _get_current_user_from_request(request)
    _require_super_admin(user)

    supabase = _get_supabase()

    payment = supabase.table("manual_payments").select("*").eq(
        "id", payment_id
    ).single().execute()

    if not payment.data:
        raise HTTPException(404, "Pago no encontrado")

    now = datetime.utcnow()

    # Reject payment
    supabase.table("manual_payments").update({
        "status": "rejected",
        "verified_by": user["email"],
        "verified_at": now.isoformat(),
        "admin_notes": body.admin_notes or "Transferencia no verificada",
        "updated_at": now.isoformat(),
    }).eq("id", payment_id).execute()

    # Downgrade org to free
    supabase.table("organizations").update({
        "plan": "free",
        "payment_method": "free",
        "plan_expires_at": None,
        "is_active": True,
        "payment_notes": f"Pago rechazado: {body.admin_notes or 'transferencia no encontrada'}",
    }).eq("id", payment.data["org_id"]).execute()

    logger.info(f"Payment rejected: {payment_id} → org downgraded to free")

    return {
        "success": True,
        "message": "Pago rechazado, organización en plan free",
        "payment_id": payment_id,
    }


# ===================================================================
# CLIENT ENDPOINT: Report bank transfer
# ===================================================================

@router.post("/api/v1/billing/transfer-claim")
async def claim_transfer(body: TransferClaim, request: "fastapi.Request"):
    """
    Client reports a bank transfer.
    Auto-activates the plan provisionally.
    Admin verifies later; if not found, plan gets suspended.
    """
    from fastapi import Request
    user = await _get_current_user_from_request(request)

    supabase = _get_supabase()

    # Validate expected amount
    plan_info = PLAN_CONFIG.get(body.plan_tier)
    if not plan_info:
        raise HTTPException(400, "Plan no válido")

    expected_amount = plan_info["price_monthly"] * body.months
    # Allow 5% tolerance for bank fees
    if body.transfer_amount < expected_amount * 0.95:
        raise HTTPException(
            400,
            f"Monto insuficiente. Plan {body.plan_tier} x {body.months} mes(es) = "
            f"${expected_amount:.2f}. Usted reportó ${body.transfer_amount:.2f}"
        )

    # Check for duplicate claims with same ref
    existing = supabase.table("manual_payments").select("id").eq(
        "transfer_ref", body.transfer_ref
    ).eq("status", "pending").execute()

    if existing.data:
        raise HTTPException(400, "Ya existe un reclamo con este código de verificación")

    # Calculate period
    now = datetime.utcnow()
    period_end = now + timedelta(days=body.months * 30)

    # Create payment record as PENDING
    payment = supabase.table("manual_payments").insert({
        "org_id": user["org_id"],
        "payment_method": "transfer",
        "amount": body.transfer_amount,
        "plan": body.plan_tier,
        "months": body.months,
        "transfer_ref": body.transfer_ref,
        "transfer_amount": body.transfer_amount,
        "transfer_bank": body.transfer_bank,
        "transfer_date": body.transfer_date,
        "transfer_verified": False,
        "status": "pending",
        "period_start": now.isoformat(),
        "period_end": period_end.isoformat(),
        "client_notes": body.client_notes,
    }).execute()

    # AUTO-ACTIVATE plan provisionally (trust but verify)
    supabase.table("organizations").update({
        "plan": body.plan_tier,
        "payment_method": "transfer",
        "plan_expires_at": period_end.isoformat(),
        "plan_started_at": now.isoformat(),
        "plan_months": body.months,
        "is_active": True,
        "payment_notes": f"Pendiente verificar: Ref {body.transfer_ref} ${body.transfer_amount}",
    }).eq("id", user["org_id"]).execute()

    logger.info(
        f"Transfer claim: org={user['org_id']}, ref={body.transfer_ref}, "
        f"${body.transfer_amount}, plan={body.plan_tier} x{body.months}m"
    )

    return {
        "success": True,
        "message": (
            f"Transferencia registrada. Plan {body.plan_tier} activado provisionalmente "
            f"por {body.months} mes(es). Verificaremos su pago en las próximas 24-48 horas."
        ),
        "payment_id": payment.data[0]["id"] if payment.data else None,
        "plan": body.plan_tier,
        "expires_at": period_end.isoformat(),
        "status": "pending_verification",
    }
