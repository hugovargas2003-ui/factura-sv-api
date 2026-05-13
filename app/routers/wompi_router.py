"""Wompi payment endpoints — credit purchases for FACTURA-SV.

POST /api/v1/payments/wompi/checkout
  Creates a hosted Wompi payment link for a given number of credits.
  Returns {payment_url, payment_id, amount, credits}.

POST /api/v1/payments/wompi/verify/{payment_id}
  Checks Wompi for settlement and, on success, credits the org's balance.
  Idempotent: re-calling after credit is granted returns success without
  double-crediting (guarded by a unique credit_transactions row keyed on
  the payment_id).
"""
from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request

from app.dependencies import get_current_user, get_dte_service
from app.services.wompi_service import (
    WompiError,
    create_payment_link,
    get_transaction,
)

logger = logging.getLogger("factura-sv.wompi_router")

router = APIRouter(prefix="/api/v1/payments/wompi", tags=["payments"])

PRICE_PER_DTE_USD = 0.10
MIN_CREDITS = 10
MAX_CREDITS = 100_000
RETURN_URL = os.getenv(
    "WOMPI_RETURN_URL",
    "https://factura-sv.algoritmos.io/dashboard/creditos",
)

_CREDIT_GRANT_MAX_RETRIES = 5


@router.post("/checkout")
async def wompi_checkout(
    request: Request,
    service=Depends(get_dte_service),
    user=Depends(get_current_user),
):
    """Create a Wompi EnlacePago for the requested number of credits."""
    body = await request.json()
    try:
        credits = int(body.get("credits", 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "credits debe ser un entero")

    if credits < MIN_CREDITS:
        raise HTTPException(400, f"Mínimo {MIN_CREDITS} créditos")
    if credits > MAX_CREDITS:
        raise HTTPException(400, f"Máximo {MAX_CREDITS} créditos por compra")

    amount = round(credits * PRICE_PER_DTE_USD, 2)
    org_id = user["org_id"]
    email = user.get("email", "")

    org = service.db.table("organizations").select("name").eq("id", org_id).single().execute()
    org_name = (org.data or {}).get("name", "") if org.data else ""

    try:
        result = await create_payment_link(
            amount_usd=amount,
            credits=credits,
            org_id=org_id,
            org_name=org_name,
            customer_email=email,
            return_url=RETURN_URL,
        )
    except WompiError as e:
        raise HTTPException(e.status, e.message)

    # Log pending purchase (balance_after stays at current value — this is
    # not yet a balance change, just an audit breadcrumb).
    try:
        current = service.db.table("organizations").select(
            "credit_balance"
        ).eq("id", org_id).single().execute()
        current_balance = (current.data or {}).get("credit_balance", 0)
        service.db.table("credit_transactions").insert({
            "org_id": org_id,
            "user_email": email,
            "amount": credits,
            "type": "wompi_pending",
            "description": f"Wompi link {result['payment_id']}: {credits} créditos (${amount})",
            "balance_after": current_balance,
            "stripe_payment_id": result["payment_id"],  # column reused as generic payment_ref
            "service": "credit_purchase",
        }).execute()
    except Exception as e:
        # Non-blocking — the payment link is already live in Wompi.
        logger.warning("Could not log pending Wompi tx: %s", e)

    return {
        "payment_url": result["payment_url"],
        "payment_id": result["payment_id"],
        "reference": result["reference"],
        "amount": amount,
        "credits": credits,
    }


@router.post("/verify/{id_transaccion}")
async def wompi_verify(
    id_transaccion: str,
    service=Depends(get_dte_service),
    user=Depends(get_current_user),
):
    """Settle a Wompi payment: credit the org and record the transaction.

    `id_transaccion` is the `idTransaccion` Wompi appends to the redirect
    URL after the customer pays (NOT the idEnlace returned by /checkout).
    We hit GET /TransaccionCompra/{id} to confirm `esReal && esAprobada`
    before granting credits.

    Idempotent — replays return success without crediting twice.
    """
    try:
        txn = await get_transaction(id_transaccion)
    except WompiError as e:
        raise HTTPException(e.status, e.message)

    if not txn["is_paid"]:
        return {
            "success": False,
            "is_paid": False,
            "message": "Pago aún no completado o no aprobado",
        }

    # Idempotency: a successful credit row carries the idTransaccion in
    # the stripe_payment_id column. If we already wrote one, short-circuit.
    existing = service.db.table("credit_transactions").select(
        "id, balance_after, amount"
    ).eq("stripe_payment_id", id_transaccion).eq("type", "purchase").execute()
    if existing.data:
        row = existing.data[0]
        return {
            "success": True,
            "already_credited": True,
            "message": "Pago ya fue acreditado previamente",
            "credits": row.get("amount"),
            "new_balance": row.get("balance_after"),
        }

    # Recover the org + credits to grant. Three sources, in priority:
    #   1. Wompi's TransaccionCompra response (infoProducto.identificadorOrg
    #      and cantidadCreditos, populated when we created the link).
    #   2. The pending row we logged at /checkout time, looked up by
    #      idEnlace echoed in the transaction.
    #   3. Fallback: caller's JWT org_id and amount / $0.10.
    target_org_id = txn.get("org_id_from_info") or ""
    credits = txn.get("credits_from_info") or 0
    id_enlace = txn.get("id_enlace") or ""

    if (not target_org_id or not credits) and id_enlace:
        pending = service.db.table("credit_transactions").select(
            "org_id, amount"
        ).eq("stripe_payment_id", id_enlace).eq("type", "wompi_pending").limit(1).execute()
        if pending.data:
            target_org_id = target_org_id or pending.data[0].get("org_id", "")
            credits = credits or int(pending.data[0].get("amount") or 0)

    if not target_org_id:
        target_org_id = user["org_id"]
    if not credits:
        # Last-resort derive from the paid amount (Wompi-authoritative).
        amount = float(txn.get("amount") or 0)
        credits = int(round(amount / PRICE_PER_DTE_USD))

    if credits <= 0:
        raise HTTPException(
            502,
            "No se pudo determinar cuántos créditos acreditar para esta transacción",
        )

    # Atomic credit grant — CAS retry, same shape as
    # DTEService._deduct_credit's deduction pattern.
    new_balance: int | None = None
    for attempt in range(_CREDIT_GRANT_MAX_RETRIES):
        org = service.db.table("organizations").select(
            "credit_balance"
        ).eq("id", target_org_id).single().execute()
        if not org.data:
            raise HTTPException(404, "Organización no encontrada")
        current = org.data.get("credit_balance") or 0
        attempted = current + credits
        update_result = service.db.table("organizations").update({
            "credit_balance": attempted,
        }).eq("id", target_org_id).eq("credit_balance", current).execute()
        if update_result.data:
            new_balance = attempted
            break
        logger.warning(
            "Wompi credit grant CAS retry %d/%d org=%s txn=%s",
            attempt + 1, _CREDIT_GRANT_MAX_RETRIES, target_org_id, id_transaccion,
        )
    else:
        logger.error(
            "Wompi credit grant gave up after %d retries org=%s txn=%s",
            _CREDIT_GRANT_MAX_RETRIES, target_org_id, id_transaccion,
        )
        raise HTTPException(
            409,
            "No se pudo acreditar por conflicto de concurrencia. "
            "Si los créditos no aparecen en 1 minuto contacte soporte.",
        )

    amount = round(credits * PRICE_PER_DTE_USD, 2)
    service.db.table("credit_transactions").insert({
        "org_id": target_org_id,
        "user_email": user.get("email", ""),
        "amount": credits,
        "type": "purchase",
        "description": (
            f"Compra Wompi: {credits} créditos (${amount}) "
            f"txn={id_transaccion} enlace={id_enlace}"
        ),
        "balance_after": new_balance,
        "stripe_payment_id": id_transaccion,  # idempotency key
        "service": "credit_purchase",
    }).execute()

    return {
        "success": True,
        "already_credited": False,
        "credits": credits,
        "new_balance": new_balance,
        "message": f"{credits} créditos acreditados",
    }
