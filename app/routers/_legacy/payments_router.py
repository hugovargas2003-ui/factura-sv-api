"""
FACTURA-SV: Manual Payments Router
===================================
Client-facing: submit transfer payment, update DTE data
Admin-facing: verify/reject transfers, register cash/check, list payments

FILE: app/routers/payments_router.py
REGISTER IN: app/main.py → app.include_router(payments_router.router, prefix="/api/v1")
"""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from supabase import Client as SupabaseClient

from app.dependencies import get_current_user, get_supabase
# plan quotas defined locally

router = APIRouter(prefix="/payments", tags=["payments"])


# ══════════════════════════════════════════════════════════════
# SCHEMAS
# ══════════════════════════════════════════════════════════════

PLAN_PRICES = {
    "emprendedor": 9.99,
    "profesional": 24.99,
    "contador": 49.99,
    "enterprise": 149.99,
}

PLAN_QUOTAS_LOCAL = {
    "free": 50,
    "emprendedor": 200,
    "profesional": 1000,
    "contador": 5000,
    "enterprise": 999999,
}

# BAC_ACCOUNT: now loaded from platform_config DB


class TransferPaymentRequest(BaseModel):
    """Client submits a transfer payment."""
    plan: str = Field(..., description="Plan: basico, profesional, enterprise")
    months: int = Field(1, ge=1, le=12)
    transfer_ref: str = Field(..., min_length=3, description="Número de confirmación BAC")
    amount: float = Field(..., gt=0)
    client_notes: Optional[str] = None


class DTEDataRequest(BaseModel):
    """Client fills DTE data for invoice generation."""
    nombre: str = Field(..., min_length=2)
    nit: Optional[str] = None
    nrc: Optional[str] = None
    giro: Optional[str] = None
    direccion: str = Field(..., min_length=5)
    departamento: str = Field(..., min_length=2)
    municipio: str = Field(..., min_length=2)
    email: Optional[str] = None
    telefono: Optional[str] = None
    tipo_documento: str = Field("nit", description="nit, dui, pasaporte")
    numero_documento: str = Field(..., min_length=5)


class AdminCashPaymentRequest(BaseModel):
    """Admin registers cash/check payment."""
    org_id: str
    plan: str
    payment_method: str = Field(..., description="cash or check")
    months: int = Field(1, ge=1, le=12)
    amount: float = Field(..., gt=0)
    admin_notes: Optional[str] = None


class AdminVerifyRequest(BaseModel):
    """Admin verifies or rejects a transfer."""
    admin_notes: Optional[str] = None


# ══════════════════════════════════════════════════════════════
# HELPER: require_admin
# ══════════════════════════════════════════════════════════════

async def require_admin(user: dict = Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "Se requieren permisos de administrador")
    return user


# ══════════════════════════════════════════════════════════════
# CLIENT ENDPOINTS
# ══════════════════════════════════════════════════════════════

@router.get("/bank-info")
async def get_bank_info(
    db: SupabaseClient = Depends(get_supabase),
):
    """Public endpoint: bank info for transfers (reads from platform_config DB)."""
    from app.services.platform_config import get_bank_info_from_config
    bank = await get_bank_info_from_config(db)
    return {
        "success": True,
        "message": "Información bancaria para transferencias",
        "data": bank,
    }


@router.post("/transfer")
async def submit_transfer_payment(
    body: TransferPaymentRequest,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Client submits transfer payment.
    - Creates manual_payments record with status=pending_verification
    - Activates plan IMMEDIATELY (provisional)
    - Admin must verify later; if rejected, plan is deactivated
    """
    if body.plan not in PLAN_PRICES:
        raise HTTPException(400, f"Plan inválido. Opciones: {list(PLAN_PRICES.keys())}")

    org_id = user.get("org_id")
    if not org_id:
        raise HTTPException(400, "Usuario no tiene organización asignada")

    # Check for duplicate transfer_ref
    existing = db.table("manual_payments").select("id").eq(
        "transfer_ref", body.transfer_ref
    ).execute()
    if existing.data:
        raise HTTPException(409, f"Ya existe un pago con referencia {body.transfer_ref}")

    # Calculate period
    from dateutil.relativedelta import relativedelta
    now = datetime.utcnow()
    period_end = now + relativedelta(months=body.months)

    # Create payment record
    payment_data = {
        "org_id": org_id,
        "payment_method": "transfer",
        "amount": body.amount,
        "plan_tier": body.plan,
        "months": body.months,
        "transfer_ref": body.transfer_ref,
        "transfer_bank": "BAC",
        "transfer_date": now.strftime("%Y-%m-%d"),
        "status": "pending_verification",
        "period_start": now.isoformat(),
        "period_end": period_end.isoformat(),
        "client_notes": body.client_notes,
    }

    payment = db.table("manual_payments").insert(payment_data).execute()
    if not payment.data:
        raise HTTPException(500, "Error creando registro de pago")

    # ACTIVATE PLAN IMMEDIATELY (provisional)
    org_update = {
        "plan": body.plan,
        "plan_status": "active",
        "payment_method": "transfer",
        "plan_expires_at": period_end.isoformat(),
        "monthly_quota": PLAN_QUOTAS_LOCAL.get(body.plan, 50),
        "is_active": True,
        "plan_started_at": now.isoformat(),
        "plan_months": body.months,
        "updated_at": now.isoformat(),
    }
    db.table("organizations").update(org_update).eq("id", org_id).execute()

    return {
        "success": True,
        "message": f"Transferencia registrada. Plan {body.plan} activado provisionalmente.",
        "data": {
            "payment_id": payment.data[0]["id"],
            "plan": body.plan,
            "status": "pending_verification",
            "expires_at": period_end.isoformat(),
            "next_step": "complete_dte_data",
        },
    }


@router.post("/{payment_id}/dte-data")
async def submit_dte_data(
    payment_id: str,
    body: DTEDataRequest,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Client fills DTE data after payment for automatic DTE emission."""
    org_id = user.get("org_id")

    # Verify payment belongs to user's org
    payment = db.table("manual_payments").select("*").eq(
        "id", payment_id
    ).eq("org_id", org_id).single().execute()

    if not payment.data:
        raise HTTPException(404, "Pago no encontrado")

    dte_data = body.dict()

    db.table("manual_payments").update({
        "dte_data": dte_data,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", payment_id).execute()

    return {
        "success": True,
        "message": "Datos para DTE guardados. Se emitirá su comprobante fiscal.",
        "data": {"payment_id": payment_id, "dte_data": dte_data},
    }


@router.get("/my-payments")
async def get_my_payments(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Client: list their org's manual payments."""
    org_id = user.get("org_id")
    if not org_id:
        raise HTTPException(400, "Usuario no tiene organización asignada")

    result = db.table("manual_payments").select("*").eq(
        "org_id", org_id
    ).order("created_at", desc=True).execute()

    return {"success": True, "data": result.data or []}


# ══════════════════════════════════════════════════════════════
# ADMIN ENDPOINTS
# ══════════════════════════════════════════════════════════════

@router.get("/admin/pending")
async def list_pending_payments(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Admin: list payments pending verification."""
    result = db.table("manual_payments").select(
        "*, organizations(name, nit, plan)"
    ).eq("status", "pending_verification").order("created_at", desc=True).execute()

    return {"success": True, "data": result.data or [], "total": len(result.data or [])}


@router.get("/admin/all")
async def list_all_payments(
    status: Optional[str] = None,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Admin: list all manual payments with optional status filter."""
    query = db.table("manual_payments").select(
        "*, organizations(name, nit, plan)"
    ).order("created_at", desc=True)

    if status:
        query = query.eq("status", status)

    result = query.execute()

    # Enrich with expiration info
    now = datetime.utcnow()
    for p in (result.data or []):
        pe = p.get("period_end")
        if pe:
            from dateutil.parser import parse as dtparse
            try:
                pe_dt = dtparse(pe).replace(tzinfo=None)
                p["_is_expired"] = pe_dt < now
                p["_days_remaining"] = max(0, (pe_dt - now).days)
            except Exception:
                p["_is_expired"] = False
                p["_days_remaining"] = None
        else:
            p["_is_expired"] = False
            p["_days_remaining"] = None

    return {"success": True, "data": result.data or [], "total": len(result.data or [])}


@router.post("/admin/verify/{payment_id}")
async def verify_payment(
    payment_id: str,
    body: AdminVerifyRequest,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Admin verifies a transfer payment.
    - Updates status to 'verified'
    - Plan stays active (already activated provisionally)
    - Marks for DTE emission
    """
    payment = db.table("manual_payments").select("*").eq(
        "id", payment_id
    ).single().execute()

    if not payment.data:
        raise HTTPException(404, "Pago no encontrado")
    if payment.data["status"] != "pending_verification":
        raise HTTPException(400, f"Pago ya tiene estado: {payment.data['status']}")

    now = datetime.utcnow()
    db.table("manual_payments").update({
        "status": "verified",
        "verified_by": admin.get("id"),
        "verified_at": now.isoformat(),
        "admin_notes": body.admin_notes,
        "updated_at": now.isoformat(),
    }).eq("id", payment_id).execute()

    return {
        "success": True,
        "message": f"Transferencia verificada. Plan confirmado para org {payment.data['org_id']}.",
    }


@router.post("/admin/reject/{payment_id}")
async def reject_payment(
    payment_id: str,
    body: AdminVerifyRequest,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Admin rejects a transfer payment.
    - Updates status to 'rejected'
    - DEACTIVATES the plan (downgrades to free)
    - Anti-fraud measure
    """
    payment = db.table("manual_payments").select("*").eq(
        "id", payment_id
    ).single().execute()

    if not payment.data:
        raise HTTPException(404, "Pago no encontrado")
    if payment.data["status"] not in ("pending_verification", "verified"):
        raise HTTPException(400, f"No se puede rechazar pago con estado: {payment.data['status']}")

    now = datetime.utcnow()

    # Reject payment
    db.table("manual_payments").update({
        "status": "rejected",
        "verified_by": admin.get("id"),
        "verified_at": now.isoformat(),
        "rejection_reason": body.admin_notes or "Transferencia no verificada",
        "admin_notes": body.admin_notes,
        "updated_at": now.isoformat(),
    }).eq("id", payment_id).execute()

    # DEACTIVATE PLAN → downgrade to free
    org_id = payment.data["org_id"]
    db.table("organizations").update({
        "plan": "free",
        "plan_status": "active",
        "payment_method": "free",
        "monthly_quota": 50,
        "plan_expires_at": None,
        "payment_notes": f"[{now.strftime('%Y-%m-%d %H:%M')}] Plan desactivado: transferencia rechazada. Ref: {payment.data.get('transfer_ref', 'N/A')}",
        "updated_at": now.isoformat(),
    }).eq("id", org_id).execute()

    return {
        "success": True,
        "message": f"Transferencia rechazada. Organización {org_id} degradada a plan free.",
    }


@router.post("/admin/cash")
async def register_cash_payment(
    body: AdminCashPaymentRequest,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Admin registers a cash/check payment.
    - Creates payment with status=active_cash (no verification needed)
    - Activates plan immediately
    """
    if body.plan not in PLAN_PRICES:
        raise HTTPException(400, f"Plan inválido. Opciones: {list(PLAN_PRICES.keys())}")
    if body.payment_method not in ("cash", "check"):
        raise HTTPException(400, "Método debe ser 'cash' o 'check'")

    # Verify org exists
    org = db.table("organizations").select("id, name").eq(
        "id", body.org_id
    ).single().execute()
    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    from dateutil.relativedelta import relativedelta
    now = datetime.utcnow()
    period_end = now + relativedelta(months=body.months)

    # Create payment record
    payment_data = {
        "org_id": body.org_id,
        "payment_method": body.payment_method,
        "amount": body.amount,
        "plan_tier": body.plan,
        "months": body.months,
        "status": "active_cash",
        "verified_by": admin.get("id"),
        "verified_at": now.isoformat(),
        "period_start": now.isoformat(),
        "period_end": period_end.isoformat(),
        "admin_notes": body.admin_notes,
    }

    payment = db.table("manual_payments").insert(payment_data).execute()

    # Activate plan
    method_label = "Efectivo" if body.payment_method == "cash" else "Cheque"
    org_update = {
        "plan": body.plan,
        "plan_status": "active",
        "payment_method": body.payment_method,
        "plan_expires_at": period_end.isoformat(),
        "monthly_quota": PLAN_QUOTAS_LOCAL.get(body.plan, 50),
        "is_active": True,
        "plan_started_at": now.isoformat(),
        "plan_months": body.months,
        "payment_notes": f"[{now.strftime('%Y-%m-%d %H:%M')}] {method_label} ${body.amount:.2f} — Plan {body.plan} x{body.months} mes(es) — Expira: {period_end.strftime('%Y-%m-%d')}",
        "updated_at": now.isoformat(),
    }
    db.table("organizations").update(org_update).eq("id", body.org_id).execute()

    return {
        "success": True,
        "message": f"Plan {body.plan} activado para {org.data['name']} hasta {period_end.strftime('%Y-%m-%d')}",
        "data": {
            "payment_id": payment.data[0]["id"] if payment.data else None,
            "org_name": org.data["name"],
            "plan": body.plan,
            "expires_at": period_end.isoformat(),
        },
    }
