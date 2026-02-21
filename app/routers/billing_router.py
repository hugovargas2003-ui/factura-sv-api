"""
FACTURA-SV: Auto-Invoicing for Subscription Payments
=====================================================
Emits a CCF (03) or Factura (01) using Efficient AI Algorithms'
own MH credentials whenever a client pays via Stripe.

CCF if client has NIT+NRC (contribuyente), Factura if not.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.services.dte_service import DTEService
from app.dependencies import get_supabase, get_encryption

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])


class AutoInvoiceRequest(BaseModel):
    """Data needed to generate a fiscal invoice for a subscription payment."""
    # Receptor (client) info
    receptor_nit: Optional[str] = None
    receptor_nrc: Optional[str] = None
    receptor_nombre: str
    receptor_email: Optional[str] = None
    receptor_telefono: Optional[str] = None
    receptor_direccion: Optional[str] = None
    receptor_departamento: Optional[str] = "06"  # San Salvador default
    receptor_municipio: Optional[str] = "14"
    receptor_actividad: Optional[str] = "62010"  # Programación informática

    # Payment details
    plan_id: str
    plan_name: str
    plan_price: float  # USD
    stripe_session_id: Optional[str] = None
    stripe_customer_id: Optional[str] = None

    # Override org_id for the CLIENT (receptor)
    client_org_id: Optional[str] = None


class AutoInvoiceResponse(BaseModel):
    success: bool
    tipo_dte: str
    codigo_generacion: Optional[str] = None
    numero_control: Optional[str] = None
    sello_recepcion: Optional[str] = None
    error: Optional[str] = None


# ============================================================
# Emisor config: Efficient AI Algorithms (Hugo's credentials)
# Loaded from environment variables for security
# ============================================================

def get_billing_emisor() -> dict:
    """Get Efficient AI Algorithms' emisor config from env."""
    return {
        "nit": os.getenv("BILLING_EMISOR_NIT", "0614-121271-103-3"),
        "nrc": os.getenv("BILLING_EMISOR_NRC", "1549809"),
        "nombre": os.getenv("BILLING_EMISOR_NOMBRE", "EFFICIENT AI ALGORITHMS"),
        "cod_actividad": os.getenv("BILLING_EMISOR_ACTIVIDAD", "62010"),
        "desc_actividad": os.getenv("BILLING_EMISOR_DESC_ACTIVIDAD",
                                     "Actividades de programación informática"),
        "nombre_comercial": "FACTURA-SV",
        "tipo_establecimiento": "20",
        "direccion_departamento": os.getenv("BILLING_EMISOR_DEPTO", "06"),
        "direccion_municipio": os.getenv("BILLING_EMISOR_MUNICIPIO", "14"),
        "direccion_complemento": os.getenv("BILLING_EMISOR_DIRECCION",
                                      "San Salvador, El Salvador"),
        "telefono": os.getenv("BILLING_EMISOR_TELEFONO", ""),
        "correo": os.getenv("BILLING_EMISOR_CORREO", "hugovargas2003@gmail.com"),
    }


def get_billing_mh_credentials() -> dict:
    """Get MH API credentials for billing emissions."""
    import base64
    pem_b64 = os.getenv("BILLING_PRIVATE_KEY_B64", "")
    pem_key = base64.b64decode(pem_b64).decode() if pem_b64 else ""
    return {
        "nit": os.getenv("BILLING_MH_NIT", ""),
        "password": os.getenv("BILLING_MH_PASSWORD", ""),
        "private_key_pem": pem_key,
    }


@router.post("/auto-invoice", response_model=AutoInvoiceResponse)
async def create_auto_invoice(
    req: AutoInvoiceRequest,
    db=Depends(get_supabase),
    encryption=Depends(get_encryption),
):
    """
    Generate a fiscal DTE for a subscription payment.

    - CCF (03) if receptor has NIT + NRC (contribuyente)
    - Factura (01) if receptor has no NIT or NRC

    Uses Efficient AI Algorithms' own MH credentials.
    Protected by internal API key (called from webhook, not user-facing).
    """
    # Verify internal API key
    # This endpoint is called by the Next.js webhook, not by users directly

    try:
        emisor = get_billing_emisor()
        mh_creds = get_billing_mh_credentials()

        if not mh_creds["nit"] or not mh_creds["password"]:
            logger.warning("Billing MH credentials not configured, skipping auto-invoice")
            return AutoInvoiceResponse(
                success=False,
                tipo_dte="",
                error="Credenciales de facturación no configuradas"
            )

        # Determine DTE type based on receptor data
        has_nit = bool(req.receptor_nit and len(req.receptor_nit) > 5)
        has_nrc = bool(req.receptor_nrc and len(req.receptor_nrc) > 0)
        tipo_dte = "03" if (has_nit and has_nrc) else "01"

        # Build receptor
        receptor = {
            "nombre": req.receptor_nombre,
            "direccion_departamento": req.receptor_departamento or "06",
            "direccion_municipio": req.receptor_municipio or "14",
            "direccion_complemento": req.receptor_direccion or "El Salvador",
        }

        if tipo_dte == "03":
            # CCF requires NIT and NRC
            receptor["nit"] = req.receptor_nit
            receptor["nrc"] = req.receptor_nrc
            receptor["cod_actividad"] = req.receptor_actividad or "62010"
            receptor["desc_actividad"] = "Servicios informáticos"
            if req.receptor_email:
                receptor["correo"] = req.receptor_email
            if req.receptor_telefono:
                receptor["telefono"] = req.receptor_telefono
        else:
            # Factura: tipo/numero documento
            if has_nit:
                receptor["tipo_documento"] = "36"
                receptor["num_documento"] = req.receptor_nit
            else:
                receptor["tipo_documento"] = "13"
                receptor["num_documento"] = "00000000-0"
            if req.receptor_email:
                receptor["correo"] = req.receptor_email

        # Build item (the subscription plan)
        precio = round(req.plan_price, 2)

        if tipo_dte == "03":
            # CCF: precio sin IVA, IVA separado
            precio_sin_iva = round(precio / 1.13, 2)
            iva = round(precio - precio_sin_iva, 2)
            item = {
                "tipo_item": 2,  # Servicio
                "descripcion": f"Suscripción mensual FACTURA-SV — Plan {req.plan_name}",
                "cantidad": 1,
                "precio_unitario": precio_sin_iva,
                "descuento": 0,
                "codigo": f"PLAN-{req.plan_id.upper()}",
                "unidad_medida": 59,  # Unidad
                "tipo_venta": 1,  # Gravada
            }
        else:
            # Factura: IVA incluido
            item = {
                "tipo_item": 2,
                "descripcion": f"Suscripción mensual FACTURA-SV — Plan {req.plan_name}",
                "cantidad": 1,
                "precio_unitario": precio,
                "descuento": 0,
                "codigo": f"PLAN-{req.plan_id.upper()}",
                "unidad_medida": 59,
                "tipo_venta": 1,
            }

        # Build full DTE payload
        now = datetime.now(timezone.utc)
        dte_payload = {
            "tipo_dte": tipo_dte,
            "emisor": emisor,
            "receptor": receptor,
            "items": [item],
            "fecha_emision": now.strftime("%Y-%m-%d"),
            "hora_emision": now.strftime("%H:%M:%S"),
            "condicion_operacion": 1,  # Contado
            "forma_pago": 5,  # Transferencia electrónica
        }

        # Use the DTE service to process, sign and transmit
        # We need to use Hugo's org credentials for MH auth
        service = DTEService(supabase=db, encryption=encryption)

        # Direct emission using billing credentials
        result = await service.emit_billing_dte(
            dte_payload=dte_payload,
            mh_credentials=mh_creds,
        )

        # Log for audit
        logger.info(
            f"Auto-invoice emitted: tipo={tipo_dte} "
            f"receptor={req.receptor_nombre} "
            f"plan={req.plan_name} amount=${req.plan_price} "
            f"stripe_session={req.stripe_session_id} "
            f"codigo={result.get('codigo_generacion', 'N/A')}"
        )

        return AutoInvoiceResponse(
            success=True,
            tipo_dte=tipo_dte,
            codigo_generacion=result.get("codigo_generacion"),
            numero_control=result.get("numero_control"),
            sello_recepcion=result.get("sello_recepcion"),
        )

    except Exception as e:
        logger.error(f"Auto-invoice failed: {e}", exc_info=True)
        return AutoInvoiceResponse(
            success=False,
            tipo_dte=tipo_dte if 'tipo_dte' in dir() else "",
            error=str(e),
        )
