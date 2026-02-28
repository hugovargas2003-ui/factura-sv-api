"""
FACTURA-SV: Auto-Invoice Helper
================================
Shared function to emit CCF/Factura after any credit purchase.
Called from: credits_router (Stripe/BAC), admin_router (cash/manual), Stripe webhook.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger("auto_invoice")


async def emit_purchase_invoice(
    supabase,
    encryption,
    org_id: str,
    cantidad: int,
    total_paid: float,
    metodo_pago: str,
    payment_ref: str = "",
) -> dict:
    """
    Emit a CCF or Factura for a credit purchase.
    
    Returns: {"success": bool, "tipo_dte": str, "codigo_generacion": str|None, "error": str|None}
    """
    try:
        # 1. Get org receptor data
        creds = supabase.table("dte_credentials").select(
            "nit, nrc, nombre_emisor, direccion_departamento, direccion_municipio, direccion_complemento"
        ).eq("org_id", org_id).limit(1).execute()

        user = supabase.table("users").select("email").eq("org_id", org_id).limit(1).execute()

        receptor_nombre = "Cliente FACTURA-SV"
        receptor_nit = None
        receptor_nrc = None
        receptor_email = None
        receptor_depto = "06"
        receptor_muni = "14"
        receptor_dir = "El Salvador"

        if creds.data and creds.data[0]:
            c = creds.data[0]
            receptor_nombre = c.get("nombre_emisor") or receptor_nombre
            receptor_nit = c.get("nit")
            receptor_nrc = c.get("nrc")
            receptor_depto = c.get("direccion_departamento") or "06"
            receptor_muni = c.get("direccion_municipio") or "14"
            receptor_dir = c.get("direccion_complemento") or "El Salvador"

        if user.data and user.data[0]:
            receptor_email = user.data[0].get("email")

        # 2. Determine DTE type
        has_nit = bool(receptor_nit and len(str(receptor_nit)) > 5)
        has_nrc = bool(receptor_nrc and len(str(receptor_nrc)) > 0)
        tipo_dte = "03" if (has_nit and has_nrc) else "01"

        # 3. Build receptor
        receptor = {
            "nombre": receptor_nombre,
            "direccion_departamento": receptor_depto,
            "direccion_municipio": receptor_muni,
            "direccion_complemento": receptor_dir,
        }

        if tipo_dte == "03":
            receptor["nit"] = receptor_nit
            receptor["nrc"] = receptor_nrc
            receptor["cod_actividad"] = "62010"
            receptor["desc_actividad"] = "Servicios informaticos"
            if receptor_email:
                receptor["correo"] = receptor_email
        else:
            if has_nit:
                receptor["tipo_documento"] = "36"
                receptor["num_documento"] = receptor_nit
            else:
                receptor["tipo_documento"] = "13"
                receptor["num_documento"] = "00000000-0"
            if receptor_email:
                receptor["correo"] = receptor_email

        # 4. Build item
        precio = round(total_paid, 2)
        if precio <= 0:
            logger.info(f"[AutoInvoice] Skipped: total_paid=${precio} for org={org_id}")
            return {"success": False, "tipo_dte": "", "error": "Monto $0 — no requiere factura"}

        desc_metodo = {
            "stripe": "Tarjeta (Stripe)",
            "transferencia_bac": "Transferencia BAC",
            "cash": "Efectivo",
            "admin_grant": "Cortesia administrativa",
        }.get(metodo_pago, metodo_pago)

        if tipo_dte == "03":
            precio_sin_iva = round(precio / 1.13, 2)
            item = {
                "tipo_item": 2,
                "descripcion": f"Creditos DTE x{cantidad} — {desc_metodo}",
                "cantidad": 1,
                "precio_unitario": precio_sin_iva,
                "descuento": 0,
                "codigo": f"CRED-{cantidad}",
                "unidad_medida": 59,
                "tipo_venta": 1,
            }
        else:
            item = {
                "tipo_item": 2,
                "descripcion": f"Creditos DTE x{cantidad} — {desc_metodo}",
                "cantidad": 1,
                "precio_unitario": precio,
                "descuento": 0,
                "codigo": f"CRED-{cantidad}",
                "unidad_medida": 59,
                "tipo_venta": 1,
            }

        # 5. Build and emit
        now = datetime.now(timezone.utc)

        from app.routers.billing_router import get_billing_emisor, get_billing_mh_credentials
        from app.services.dte_service import DTEService

        emisor = await get_billing_emisor(supabase)
        mh_creds = get_billing_mh_credentials()

        if not mh_creds.get("nit") or not mh_creds.get("password"):
            logger.warning(f"[AutoInvoice] Billing MH creds not configured for org={org_id}")
            return {"success": False, "tipo_dte": tipo_dte, "error": "Credenciales de facturacion no configuradas"}

        dte_payload = {
            "tipo_dte": tipo_dte,
            "emisor": emisor,
            "receptor": receptor,
            "items": [item],
            "fecha_emision": now.strftime("%Y-%m-%d"),
            "hora_emision": now.strftime("%H:%M:%S"),
            "condicion_operacion": 1,
            "observaciones": f"Compra de {cantidad} creditos DTE. {desc_metodo}. Ref: {payment_ref}",
            "forma_pago": 5 if metodo_pago in ("stripe", "transferencia_bac") else 1,
        }

        service = DTEService(supabase=supabase, encryption=encryption)
        result = await service.emit_billing_dte(
            dte_payload=dte_payload,
            mh_credentials=mh_creds,
        )

        codigo = result.get("codigo_generacion", "")
        logger.info(
            f"[AutoInvoice] OK: org={org_id} tipo={tipo_dte} qty={cantidad} "
            f"total=${total_paid} metodo={metodo_pago} codigo={codigo}"
        )

        # 6. Update credit_transaction with invoice reference
        if codigo:
            try:
                supabase.table("credit_transactions").update({
                    "invoice_codigo": codigo,
                    "invoice_tipo": tipo_dte,
                }).eq("org_id", org_id).eq(
                    "type", "purchase"
                ).order("created_at", desc=True).limit(1).execute()
            except Exception as e:
                logger.warning(f"[AutoInvoice] Could not update tx with invoice ref: {e}")

        return {
            "success": True,
            "tipo_dte": tipo_dte,
            "codigo_generacion": codigo,
            "numero_control": result.get("numero_control"),
            "sello_recepcion": result.get("sello_recepcion"),
        }

    except Exception as e:
        logger.error(f"[AutoInvoice] FAILED: org={org_id} qty={cantidad} error={e}", exc_info=True)
        return {"success": False, "tipo_dte": "", "error": str(e)}
