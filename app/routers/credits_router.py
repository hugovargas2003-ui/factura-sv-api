"""
credits_router.py — Credit system endpoints for DTE prepaid credits.
Implements the logarithmic pricing algorithm from FACTURASV_Algoritmo_Precios_Creditos_DTE.pdf

Formula: precio_por_dte = max(P_min, P_base - K * ln(cantidad))
Parameters stored in platform_config table (Supabase).
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
import math
import logging

from app.dependencies import get_supabase, get_current_user

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
    metodo_pago: str = Field(..., pattern="^(stripe|transferencia_bac)$")
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

def get_pricing_params(supabase) -> dict:
    """Fetch pricing parameters from platform_config."""
    keys = [
        'pricing_p_base', 'pricing_p_min', 'pricing_k',
        'pricing_min_recharge', 'pricing_alert_pct', 'pricing_alert_critical',
        'pricing_trial_credits', 'pricing_trial_days'
    ]
    result = supabase.table("platform_config").select("key, value").in_("key", keys).execute()
    params = {row["key"]: row["value"] for row in (result.data or [])}
    return {
        "p_base": float(params.get("pricing_p_base", "0.25")),
        "p_min": float(params.get("pricing_p_min", "0.04")),
        "k": float(params.get("pricing_k", "0.022")),
        "min_recharge": int(params.get("pricing_min_recharge", "10")),
        "alert_pct": int(params.get("pricing_alert_pct", "20")),
        "alert_critical": int(params.get("pricing_alert_critical", "5")),
        "trial_credits": int(params.get("pricing_trial_credits", "50")),
        "trial_days": int(params.get("pricing_trial_days", "3")),
    }


def calculate_price(cantidad: int, p_base: float, p_min: float, k: float) -> tuple:
    """
    Logarithmic pricing: precio = max(P_min, P_base - K * ln(cantidad))
    Returns (unit_price, total, discount_pct)
    """
    if cantidad <= 0:
        return (p_base, 0.0, 0.0)
    raw_price = p_base - k * math.log(cantidad)
    unit_price = round(max(p_min, raw_price), 4)
    total = round(unit_price * cantidad, 2)
    discount_pct = round((1 - unit_price / p_base) * 100, 1)
    return (unit_price, total, discount_pct)


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

    params = get_pricing_params(supabase)
    unit_price, total, discount_pct = calculate_price(
        cantidad, params["p_base"], params["p_min"], params["k"]
    )
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
):
    """Purchase DTE credits. Calculates price dynamically."""
    params = get_pricing_params(supabase)

    if req.cantidad < params["min_recharge"]:
        raise HTTPException(400, f"Minimo {params['min_recharge']} creditos por recarga")

    unit_price, total, _ = calculate_price(
        req.cantidad, params["p_base"], params["p_min"], params["k"]
    )

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

    # Record transaction
    tx = supabase.table("credit_transactions").insert({
        "org_id": org_id,
        "type": "purchase",
        "amount": req.cantidad,
        "balance": new_balance,
        "unit_price": unit_price,
        "total_paid": total,
        "payment_ref": req.payment_ref or f"{req.metodo_pago}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
    }).execute()

    logger.info(f"Credits purchased: org={org_id} qty={req.cantidad} total=${total} balance={new_balance}")

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
