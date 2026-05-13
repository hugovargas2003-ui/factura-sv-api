"""Wompi El Salvador payment integration.

API reference (verified 2026-05-12 against docs.wompi.sv):

  Auth — https://docs.wompi.sv/autenticacion/autenticacion
    POST https://id.wompi.sv/connect/token
    Body (application/x-www-form-urlencoded):
      grant_type=client_credentials
      client_id=<WOMPI_APP_ID>
      client_secret=<WOMPI_API_SECRET>
      audience=wompi_api
    Returns {access_token, expires_in (seconds), token_type, scope}.

  Create payment link — https://docs.wompi.sv/metodos-api/enlace-de-pago
    POST https://api.wompi.sv/EnlacePago    (SINGULAR — plural 404s)
    Body (application/json) — minimum:
      identificadorEnlaceComercio: str
      monto: number  (>= 0.01 USD)
      nombreProducto: str
    Optional: descripcion, formaPago, configuracion (urlRedirect, ...),
              infoProducto (custom merchant fields).
    Returns urlEnlace, idEnlace, urlQrCodeEnlace.

  Query transaction — https://docs.wompi.sv/redirect-url/parametros-de-url-de-redirect
    GET https://api.wompi.sv/TransaccionCompra/{idTransaccion}
    Authoritative source of payment outcome. A transaction is settled
    when BOTH `esReal == true` AND `esAprobada == true`.
    `idTransaccion` arrives in the redirect URL after the customer pays.

Env vars:
  WOMPI_APP_ID
  WOMPI_API_SECRET
  WOMPI_BASE_URL        (default https://api.wompi.sv)
  WOMPI_ID_URL          (default https://id.wompi.sv)
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

    Reuses the in-process cache until 60s before declared expiry to
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
        logger.error(
            "Wompi auth failed: status=%s body=%s",
            resp.status_code, resp.text[:500],
        )
        raise WompiError(
            f"Wompi auth failed (HTTP {resp.status_code})",
            status=502,
            raw={"body": resp.text[:500]},
        )

    body = resp.json()
    token = body.get("access_token")
    expires_in = int(body.get("expires_in", 3600))
    if not token:
        logger.error("Wompi auth: response missing access_token: %s", body)
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
    # Wompi rejects EnlacePago creation unless at least one notification
    # channel is configured — either a webhook or emailsNotificacion.
    # We don't run a Wompi webhook yet, so we attach the customer email
    # (falling back to support so the link still creates if the user has
    # no email on file).
    notify_email = (customer_email or "").strip() or "contacto@algoritmos.io"
    token = await _get_access_token()

    # Per docs, minimum body is identificadorEnlaceComercio + monto +
    # nombreProducto. configuracion/infoProducto/formaPago are optional
    # but documented to be accepted. Custom data on infoProducto comes
    # back in the redirect URL as identificadorEnlaceComercio + idEnlace.
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
            "urlRedirect": f"{return_url}?status=success&credits={credits}",
            "esMontoEditable": False,
            "esCantidadEditable": False,
            "cantidadPorDefecto": 1,
            "duracionInactividad": 10,
            "emailsNotificacion": notify_email,
        },
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
                f"{WOMPI_BASE_URL}/EnlacePago",
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
        logger.error(
            "Wompi create link failed: status=%s body=%s payload_keys=%s",
            resp.status_code, resp.text[:500], list(payload.keys()),
        )
        raise WompiError(
            f"Wompi rechazó el enlace (HTTP {resp.status_code})",
            status=502,
            raw={"body": resp.text[:500]},
        )

    data = resp.json()
    payment_url = data.get("urlEnlace") or data.get("urlCompleta") or data.get("url")
    payment_id = data.get("idEnlace") or data.get("id")
    if not payment_url or not payment_id:
        logger.error("Wompi create link OK but missing url/id: %s", data)
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


async def get_transaction(id_transaccion: str) -> dict:
    """Query a settled transaction by Wompi's idTransaccion.

    `idTransaccion` is what Wompi appends to the redirect URL after the
    customer pays — NOT the idEnlace we stored from create_payment_link.

    Returns:
      is_paid    — True iff esReal && esAprobada (Wompi's settled flag).
      amount     — float, the actual paid monto (authoritative).
      id_enlace  — str, the EnlacePago this transaction came from.
      reference  — str, our identificadorEnlaceComercio (echoed back).
      raw        — full Wompi response.

    Raises WompiError on transport, auth, or 404.
    """
    if not id_transaccion:
        raise WompiError("idTransaccion vacío", status=400)
    token = await _get_access_token()

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{WOMPI_BASE_URL}/TransaccionCompra/{id_transaccion}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
            )
    except httpx.HTTPError as e:
        raise WompiError(f"No se pudo conectar a Wompi: {e}", status=502) from e

    if resp.status_code == 404:
        raise WompiError("Transacción no encontrada en Wompi", status=404)
    if resp.status_code != 200:
        logger.error(
            "Wompi GET transaction failed: status=%s body=%s id=%s",
            resp.status_code, resp.text[:500], id_transaccion,
        )
        raise WompiError(
            f"Wompi rechazó la consulta (HTTP {resp.status_code})",
            status=502,
            raw={"body": resp.text[:500]},
        )

    data = resp.json()
    # Per docs: payment is settled when BOTH flags are true.
    es_real = _truthy(data.get("esReal"))
    es_aprobada = _truthy(data.get("esAprobada"))
    is_paid = es_real and es_aprobada

    # The link metadata may be nested under enlacePago / enlace, or
    # echoed at the top level depending on response variant.
    enlace = (
        data.get("enlacePago")
        or data.get("enlace")
        or data
    )
    id_enlace = (
        data.get("idEnlace")
        or (enlace.get("idEnlace") if isinstance(enlace, dict) else None)
    )
    reference = (
        data.get("identificadorEnlaceComercio")
        or (enlace.get("identificadorEnlaceComercio") if isinstance(enlace, dict) else None)
    )

    info = (data.get("infoProducto") or (enlace.get("infoProducto") if isinstance(enlace, dict) else {}) or {})
    try:
        credits = int(info.get("cantidadCreditos") or 0)
    except (ValueError, TypeError):
        credits = 0

    return {
        "is_paid": is_paid,
        "amount": float(data.get("monto") or 0),
        "id_enlace": str(id_enlace) if id_enlace is not None else "",
        "reference": str(reference) if reference is not None else "",
        "org_id_from_info": info.get("identificadorOrg") or "",
        "credits_from_info": credits,
        "raw": data,
    }


# Back-compat shim — older callers used `verify_payment(id)` expecting
# the EnlacePago-keyed verify. The argument is now treated as the
# Wompi idTransaccion (from the redirect URL).
async def verify_payment(id_transaccion: str) -> dict:
    txn = await get_transaction(id_transaccion)
    return {
        "is_paid": txn["is_paid"],
        "credits": txn["credits_from_info"],
        "org_id": txn["org_id_from_info"],
        "amount": txn["amount"],
        "raw": txn["raw"],
    }


def _truthy(value) -> bool:
    """Wompi sends booleans as either real bools or the strings 'true'/'false'."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return False
