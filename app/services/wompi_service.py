"""Wompi El Salvador payment integration.

Auth flow (OAuth2 client credentials — different from Wompi Colombia):

  1.  POST https://id.wompi.sv/connect/token
      form-urlencoded: grant_type=client_credentials,
                       audience=wompi_api,
                       client_id=<WOMPI_APP_ID>,
                       client_secret=<WOMPI_API_SECRET>
      → {access_token, expires_in, token_type}

  2.  Calls to api.wompi.sv go with `Authorization: Bearer <access_token>`.

Tokens are cached in-process until ~60s before their declared expiry.

Env:
  WOMPI_APP_ID
  WOMPI_API_SECRET
  WOMPI_BASE_URL        (optional, default api.wompi.sv)
  WOMPI_ID_URL          (optional, default id.wompi.sv)
"""
from __future__ import annotations

import os
import time
import logging
from typing import Optional

import httpx

logger = logging.getLogger("factura-sv.wompi")

WOMPI_ID_URL = os.getenv("WOMPI_ID_URL", "https://id.wompi.sv")
WOMPI_BASE_URL = os.getenv("WOMPI_BASE_URL", "https://api.wompi.sv")


class WompiError(Exception):
    """Wompi API failure surfaced for HTTP responses."""

    def __init__(self, message: str, status: int = 502, raw: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.status = status
        self.raw = raw or {}


# ── Token cache ──────────────────────────────────────────────────

_token_cache: dict = {"access_token": None, "expires_at": 0.0}


async def _get_access_token() -> str:
    """Fetch (or reuse) a Wompi OAuth2 access token.

    Re-uses the in-process cache until 60s before declared expiry to
    avoid hammering the auth endpoint on every payment.
    """
    now = time.monotonic()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    app_id = os.getenv("WOMPI_APP_ID", "")
    secret = os.getenv("WOMPI_API_SECRET", "")
    if not app_id or not secret:
        raise WompiError(
            "Wompi no configurado: faltan WOMPI_APP_ID / WOMPI_API_SECRET",
            status=500,
        )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{WOMPI_ID_URL}/connect/token",
                data={
                    "grant_type": "client_credentials",
                    "audience": "wompi_api",
                    "client_id": app_id,
                    "client_secret": secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
    except httpx.HTTPError as e:
        raise WompiError(f"No se pudo conectar a Wompi auth: {e}", status=502) from e

    if resp.status_code != 200:
        logger.error("Wompi token failed: %s %s", resp.status_code, resp.text[:400])
        raise WompiError(
            f"Wompi auth failed (HTTP {resp.status_code})",
            status=502,
            raw={"body": resp.text[:400]},
        )

    body = resp.json()
    token = body.get("access_token")
    expires_in = int(body.get("expires_in", 3600))
    if not token:
        raise WompiError("Wompi auth: respuesta sin access_token", status=502, raw=body)

    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + expires_in
    return token


def _reset_token_cache() -> None:
    """Test helper — force the next call to re-authenticate."""
    _token_cache["access_token"] = None
    _token_cache["expires_at"] = 0.0


# ── Public API ───────────────────────────────────────────────────

async def create_payment_link(
    *,
    amount_usd: float,
    credits: int,
    org_id: str,
    org_name: str,
    customer_email: str,
    return_url: str,
) -> dict:
    """Create a Wompi EnlacePago for a one-shot credit purchase.

    Returns: {"payment_url", "payment_id", "reference"}.
    Raises WompiError on any failure — callers translate to HTTPException.
    """
    if amount_usd <= 0 or credits <= 0:
        raise WompiError("Monto y créditos deben ser positivos", status=400)

    reference = f"FSV-{credits}cr-{org_id[:8]}-{int(time.time())}"
    token = await _get_access_token()

    payload = {
        "identificadorEnlaceComercio": reference,
        "monto": round(amount_usd, 2),
        "nombreProducto": f"{credits} Créditos DTE — FACTURA-SV",
        "descripcion": (
            f"{credits} créditos DTE a $0.10 c/u. Incluye emisión MH + PDF + "
            f"email + WhatsApp. Los créditos no expiran."
        ),
        "formaPago": {
            "permitirTarjetaCreditoDebido": True,
            "permitirPagoConPuntoAgricola": False,
            "permitirPagoEnCuotasAgricola": False,
            "permitirPagoConCreditoEnLinea": False,
        },
        "configuracion": {
            # Wompi appends its own params; this URL is where the customer
            # lands after the hosted checkout closes.
            "urlRedirect": f"{return_url}?status=success&credits={credits}&ref={reference}",
            "esMontoEditable": False,
            "esCantidadEditable": False,
            "cantidadPorDefecto": 1,
            "duracionInactividad": 10,
        },
        # Wompi mirrors infoProducto fields back on the verify call —
        # we use them to recover org_id + credits without trusting the
        # browser's URL.
        "infoProducto": {
            "nombreCliente": org_name,
            "correoCliente": customer_email,
            "identificadorOrg": org_id,
            "cantidadCreditos": str(credits),
        },
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{WOMPI_BASE_URL}/EnlacesPago",
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
    except httpx.HTTPError as e:
        raise WompiError(f"No se pudo conectar a Wompi: {e}", status=502) from e

    if resp.status_code not in (200, 201):
        logger.error("Wompi create link failed: %s %s", resp.status_code, resp.text[:400])
        raise WompiError(
            f"Wompi rechazó el enlace (HTTP {resp.status_code})",
            status=502,
            raw={"body": resp.text[:400]},
        )

    data = resp.json()
    # Wompi SV docs alternate between `urlEnlace` and `urlCompleta`; accept both.
    payment_url = (
        data.get("urlCompleta")
        or data.get("urlEnlace")
        or data.get("url")
    )
    payment_id = data.get("idEnlace") or data.get("id")
    if not payment_url or not payment_id:
        raise WompiError(
            "Wompi devolvió respuesta sin URL o ID",
            status=502,
            raw=data,
        )

    return {
        "payment_url": payment_url,
        "payment_id": str(payment_id),
        "reference": reference,
    }


async def verify_payment(payment_id: str) -> dict:
    """Look up a payment link to see whether it has been settled.

    Returns dict with:
      is_paid          — bool
      credits          — int (from infoProducto.cantidadCreditos)
      org_id           — str (from infoProducto.identificadorOrg)
      amount           — float (from the link's monto)
      raw              — full Wompi response for debugging
    """
    token = await _get_access_token()

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{WOMPI_BASE_URL}/EnlacesPago/{payment_id}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
            )
    except httpx.HTTPError as e:
        raise WompiError(f"No se pudo conectar a Wompi: {e}", status=502) from e

    if resp.status_code == 404:
        raise WompiError("Pago no encontrado en Wompi", status=404)
    if resp.status_code != 200:
        raise WompiError(
            f"Wompi rechazó la consulta (HTTP {resp.status_code})",
            status=502,
            raw={"body": resp.text[:400]},
        )

    data = resp.json()

    # Wompi nests the transaction outcome under "transaccionCompra" /
    # "transaccionPago" depending on link type. Inspect both.
    txn = (
        data.get("transaccionCompra")
        or data.get("transaccionPago")
        or data.get("transaccion")
        or {}
    )
    estado = ""
    if isinstance(txn, dict):
        resultado = txn.get("resultado") or {}
        estado = (resultado.get("estado") or resultado.get("estadoTransaccion") or "").lower()
    is_paid = estado in ("aprobada", "approved", "completada", "exitosa", "pagada")

    info = data.get("infoProducto") or {}
    try:
        credits = int(info.get("cantidadCreditos") or 0)
    except (ValueError, TypeError):
        credits = 0
    org_id = info.get("identificadorOrg") or ""

    return {
        "is_paid": is_paid,
        "credits": credits,
        "org_id": org_id,
        "amount": float(data.get("monto") or 0),
        "raw": data,
    }
