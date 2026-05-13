"""
credits_router.py — Credit system endpoints for DTE prepaid credits.

Flat pricing: $0.10 per DTE credit.
Each credit includes: DTE emission + MH transmission + digital signature + PDF
+ email delivery + WhatsApp delivery.
Credits never expire. No monthly fees.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
import logging

from app.dependencies import get_supabase, get_current_user, get_encryption

logger = logging.getLogger("credits")
router = APIRouter(prefix="/api/v1", tags=["credits"])


# ── Models ──

class PricingRequest(BaseModel):
    cantidad: int = Field(..., ge=10, description="Number of credits to calculate")

class PricingResponse(BaseModel):
    cantidad: int
    precio_unitario: float
    total: float
    descuento_pct: float

class PurchaseRequest(BaseModel):
    cantidad: int = Field(..., ge=10)
    metodo_pago: str = Field(..., pattern="^(wompi|stripe|transferencia_bac)$")
    payment_ref: str | None = None

class PurchaseResponse(BaseModel):
    credits_added: int
    new_balance: int
    amount_charged: float
    unit_price: float
    receipt_id: str | None = None

class BalanceResponse(BaseModel):
    credit_balance: int
    plan: str
    plan_status: str
    alert_level: str | None = None  # "yellow" | "red" | None

class CreditTransaction(BaseModel):
    id: str
    type: str
    amount: int
    balance: int
    unit_price: float | None
    total_paid: float | None
    payment_ref: str | None
    created_at: str


# ── Helpers ──

PRICE_PER_DTE = 0.10


def get_pricing_params(supabase) -> dict:
    """Fetch non-pricing params (recharge minimum, alerts, trial) from platform_config.

    Pricing itself is hardcoded at $0.10/DTE — not configurable via DB.
    """
    keys = [
        'pricing_min_recharge', 'pricing_alert_pct', 'pricing_alert_critical',
        'pricing_trial_credits', 'pricing_trial_days',
    ]
    result = supabase.table("platform_config").select("key, value").in_("key", keys).execute()
    params = {row["key"]: row["value"] for row in (result.data or [])}
    return {
        "min_recharge": int(params.get("pricing_min_recharge", "10")),
        "alert_pct": int(params.get("pricing_alert_pct", "20")),
        "alert_critical": int(params.get("pricing_alert_critical", "5")),
        "trial_credits": int(params.get("pricing_trial_credits", "10")),
        "trial_days": int(params.get("pricing_trial_days", "3")),
    }


def calculate_price(cantidad: int) -> tuple:
    """
    Flat pricing: $0.10 per DTE credit.
    Includes emission + MH transmission + digital signature + PDF + email + WhatsApp.
    Credits never expire. No monthly fees. No volume discount.
    Returns (unit_price, total, discount_pct) — discount_pct always 0.
    """
    if cantidad <= 0:
        return (PRICE_PER_DTE, 0.0, 0.0)
    total = round(cantidad * PRICE_PER_DTE, 2)
    return (PRICE_PER_DTE, total, 0.0)


# ── Public Endpoint ──

@router.get("/pricing/calculate", response_model=PricingResponse)
async def pricing_calculate(cantidad: int = 100, supabase=Depends(get_supabase)):
    """
    Public endpoint — calculates price for X credits.
    No authentication required.
    """
    if cantidad < 10:
        raise HTTPException(400, "Minimo 10 creditos por recarga")
    if cantidad > 100000:
        raise HTTPException(400, "Para mas de 100,000 creditos contacte ventas")

    unit_price, total, discount_pct = calculate_price(cantidad)
    return PricingResponse(
        cantidad=cantidad,
        precio_unitario=unit_price,
        total=total,
        descuento_pct=discount_pct,
    )


# ── Authenticated Endpoints ──

@router.get("/credits/balance", response_model=BalanceResponse)
async def get_balance(user=Depends(get_current_user), supabase=Depends(get_supabase)):
    """Get current credit balance for the user's organization."""
    org = supabase.table("organizations").select(
        "credit_balance, plan, plan_status"
    ).eq("id", user["org_id"]).single().execute()

    if not org.data:
        raise HTTPException(404, "Organizacion no encontrada")

    balance = org.data["credit_balance"]
    params = get_pricing_params(supabase)

    alert_level = None
    if balance <= params["alert_critical"]:
        alert_level = "red"
    elif balance > 0:
        # Check against last purchase to determine yellow alert
        last_purchase = supabase.table("credit_transactions").select(
            "amount"
        ).eq("org_id", user["org_id"]).eq(
            "type", "purchase"
        ).order("created_at", desc=True).limit(1).execute()

        if last_purchase.data:
            last_amount = last_purchase.data[0]["amount"]
            if balance <= last_amount * params["alert_pct"] / 100:
                alert_level = "yellow"

    return BalanceResponse(
        credit_balance=balance,
        plan=org.data["plan"],
        plan_status=org.data["plan_status"],
        alert_level=alert_level,
    )


@router.post("/credits/purchase", response_model=PurchaseResponse)
async def purchase_credits(
    req: PurchaseRequest,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
    encryption=Depends(get_encryption),
):
    """Purchase DTE credits. Calculates price dynamically."""
    params = get_pricing_params(supabase)

    if req.cantidad < params["min_recharge"]:
        raise HTTPException(400, f"Minimo {params['min_recharge']} creditos por recarga")

    unit_price, total, _ = calculate_price(req.cantidad)

    org_id = user["org_id"]

    # Get current balance
    org = supabase.table("organizations").select(
        "credit_balance"
    ).eq("id", org_id).single().execute()

    if not org.data:
        raise HTTPException(404, "Organizacion no encontrada")

    current_balance = org.data["credit_balance"]
    new_balance = current_balance + req.cantidad

    # Update balance
    supabase.table("organizations").update({
        "credit_balance": new_balance,
    }).eq("id", org_id).execute()

    payment_ref = req.payment_ref or f"{req.metodo_pago}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    # Record transaction
    tx = supabase.table("credit_transactions").insert({
        "user_email": user["email"],
        "type": "purchase",
        "amount": req.cantidad,
        "balance_after": new_balance,
        "description": f"unit=${unit_price:.4f} total=${total:.2f} ref={payment_ref}",
        "service": "credits_purchase",
        "stripe_payment_id": req.payment_ref if req.payment_ref else None,
    }).execute()

    logger.info(f"Credits purchased: org={org_id} qty={req.cantidad} total=${total} balance={new_balance}")

    # ── Auto-emit CCF/Factura for the purchase ──
    invoice_result = {"success": False}
    try:
        from app.services.auto_invoice_helper import emit_purchase_invoice
        invoice_result = await emit_purchase_invoice(
            supabase=supabase,
            encryption=encryption,
            org_id=org_id,
            cantidad=req.cantidad,
            total_paid=total,
            metodo_pago=req.metodo_pago,
            payment_ref=payment_ref,
        )
        if invoice_result.get("success"):
            logger.info(f"Auto-invoice OK: {invoice_result.get('codigo_generacion')}")
        else:
            logger.warning(f"Auto-invoice skipped/failed: {invoice_result.get('error')}")
    except Exception as e:
        logger.error(f"Auto-invoice error (non-blocking): {e}")

    return PurchaseResponse(
        credits_added=req.cantidad,
        new_balance=new_balance,
        amount_charged=total,
        unit_price=unit_price,
        receipt_id=tx.data[0]["id"] if tx.data else None,
    )


@router.get("/credits/history")
async def credit_history(
    page: int = 1,
    per_page: int = 20,
    type_filter: str | None = None,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Get credit transaction history with pagination."""
    org_id = user["org_id"]
    query = supabase.table("credit_transactions").select(
        "id, type, amount, balance, unit_price, total_paid, payment_ref, created_at",
        count="exact",
    ).eq("org_id", org_id)

    if type_filter:
        query = query.eq("type", type_filter)

    offset = (page - 1) * per_page
    result = query.order("created_at", desc=True).range(offset, offset + per_page - 1).execute()

    return {
        "data": result.data or [],
        "total": result.count if hasattr(result, 'count') else len(result.data or []),
        "page": page,
        "per_page": per_page,
    }
