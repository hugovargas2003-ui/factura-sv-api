"""
FACTURA-SV: Email Service via Resend
======================================
Replaces Google Apps Script (2K/day limit) with Resend (100K/month free tier).
Supports PDF + JSON attachments for DTE delivery.

Fallback chain: Custom SMTP → Resend → GAS legacy
"""
import base64
import logging
import os

logger = logging.getLogger("factura-sv")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")


async def send_dte_email_resend(
    to_email: str,
    subject: str,
    html_body: str,
    pdf_bytes: bytes = None,
    pdf_filename: str = None,
    json_bytes: bytes = None,
    json_filename: str = None,
    from_email: str = None,
    from_name: str = None,
) -> dict:
    """Send DTE email with PDF + JSON attachments via Resend."""
    import resend

    resend.api_key = RESEND_API_KEY

    attachments = []
    if pdf_bytes:
        attachments.append({
            "filename": pdf_filename or "dte.pdf",
            "content": base64.b64encode(pdf_bytes).decode("ascii"),
        })
    if json_bytes:
        attachments.append({
            "filename": json_filename or "dte.json",
            "content": base64.b64encode(json_bytes).decode("ascii"),
        })

    try:
        sender = f"{from_name or 'FACTURA-SV'} <{from_email or 'noreply@algoritmos.io'}>"
        params: dict = {
            "from": sender,
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        }
        if attachments:
            params["attachments"] = attachments

        result = resend.Emails.send(params)
        email_id = result.get("id", "") if isinstance(result, dict) else str(result)
        logger.info(f"Email sent via Resend: {email_id} to {to_email}")
        return {"success": True, "id": email_id}
    except Exception as e:
        logger.error(f"Resend email failed: {e}")
        return {"success": False, "error": str(e)}


def _build_dte_html(
    receptor_nombre: str,
    emisor_nombre: str,
    tipo_nombre: str,
    numero_control: str,
    codigo_generacion: str,
    fecha_emision: str,
    monto_total: float,
    sello_recepcion: str = "",
) -> str:
    """Build a professional HTML email body for DTE delivery."""
    return f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
      <div style="background: linear-gradient(135deg, #4f46e5, #3730a3); padding: 24px; border-radius: 12px 12px 0 0; text-align: center;">
        <h1 style="color: white; font-size: 20px; margin: 0;">FACTURA-SV</h1>
        <p style="color: #c7d2fe; font-size: 12px; margin: 4px 0 0;">Documento Tributario Electronico</p>
      </div>
      <div style="background: #ffffff; padding: 24px; border: 1px solid #e5e7eb; border-top: none;">
        <p style="color: #374151; font-size: 14px;">Estimado/a <strong>{receptor_nombre}</strong>,</p>
        <p style="color: #6b7280; font-size: 13px;">
          Se adjunta su <strong>{tipo_nombre}</strong> emitido por <strong>{emisor_nombre}</strong>.
        </p>
        <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
          <tr><td style="padding: 8px; color: #6b7280; font-size: 12px; border-bottom: 1px solid #f3f4f6;">Numero de Control</td>
              <td style="padding: 8px; color: #111827; font-size: 13px; font-weight: 600; border-bottom: 1px solid #f3f4f6; text-align: right;">{numero_control}</td></tr>
          <tr><td style="padding: 8px; color: #6b7280; font-size: 12px; border-bottom: 1px solid #f3f4f6;">Codigo de Generacion</td>
              <td style="padding: 8px; color: #111827; font-size: 11px; font-family: monospace; border-bottom: 1px solid #f3f4f6; text-align: right;">{codigo_generacion[:20]}...</td></tr>
          <tr><td style="padding: 8px; color: #6b7280; font-size: 12px; border-bottom: 1px solid #f3f4f6;">Fecha</td>
              <td style="padding: 8px; color: #111827; font-size: 13px; border-bottom: 1px solid #f3f4f6; text-align: right;">{fecha_emision}</td></tr>
          <tr><td style="padding: 8px; color: #6b7280; font-size: 12px;">Total</td>
              <td style="padding: 8px; color: #4f46e5; font-size: 18px; font-weight: 700; text-align: right;">${monto_total:.2f}</td></tr>
        </table>
        {f'<p style="color: #6b7280; font-size: 11px;">Sello de recepcion MH: <code style="font-size: 10px;">{sello_recepcion[:30]}...</code></p>' if sello_recepcion else ''}
        <p style="color: #6b7280; font-size: 12px; margin-top: 16px;">
          Puede verificar este documento en:
          <a href="https://admin.factura.gob.sv/consultaPublica" style="color: #4f46e5;">admin.factura.gob.sv/consultaPublica</a>
        </p>
      </div>
      <div style="background: #f9fafb; padding: 16px; border-radius: 0 0 12px 12px; border: 1px solid #e5e7eb; border-top: none; text-align: center;">
        <p style="color: #9ca3af; font-size: 10px; margin: 0;">Emitido via FACTURA-SV | algoritmos.io</p>
      </div>
    </div>
    """


DTE_NOMBRES = {
    "01": "Factura", "03": "Comprobante de Credito Fiscal",
    "04": "Nota de Remision", "05": "Nota de Credito",
    "06": "Nota de Debito", "07": "Comprobante de Retencion",
    "08": "Comprobante de Liquidacion", "09": "Doc. Contable de Liquidacion",
    "11": "Factura de Exportacion", "14": "Factura de Sujeto Excluido",
    "15": "Comprobante de Donacion",
}


async def send_email_with_fallback(
    to_email: str,
    receptor_nombre: str,
    emisor_nombre: str,
    tipo_dte: str,
    numero_control: str,
    codigo_generacion: str,
    sello_recepcion: str,
    monto_total: float,
    fecha_emision: str,
    pdf_bytes: bytes,
    dte_json: dict,
    org_id: str = None,
) -> bool:
    """
    Send DTE email with fallback chain:
    1. Custom SMTP (if org has config)
    2. Resend (if RESEND_API_KEY set)
    3. GAS legacy
    """
    if not to_email:
        logger.warning(f"DTE {codigo_generacion[:8]}: sin email de receptor")
        return False

    import json

    tipo_nombre = DTE_NOMBRES.get(tipo_dte, tipo_dte)
    filename_base = f"DTE-{tipo_dte}-{numero_control.replace('/', '-')}"
    json_bytes_encoded = json.dumps(dte_json, ensure_ascii=False, indent=2).encode("utf-8")

    # 1. Try custom SMTP (if org has config)
    if org_id:
        try:
            from app.dependencies import get_supabase
            db = get_supabase()
            smtp_result = db.table("org_email_config").select("*").eq(
                "org_id", org_id
            ).eq("use_custom_email", True).eq("is_verified", True).limit(1).execute()
            if smtp_result.data:
                from app.services.email_service import _send_via_custom_smtp
                sent = await _send_via_custom_smtp(
                    smtp_config=smtp_result.data[0],
                    receptor_email=to_email,
                    receptor_nombre=receptor_nombre,
                    emisor_nombre=emisor_nombre,
                    tipo_dte=tipo_dte,
                    numero_control=numero_control,
                    codigo_generacion=codigo_generacion,
                    monto_total=monto_total,
                    fecha_emision=fecha_emision,
                    pdf_bytes=pdf_bytes,
                    dte_json=dte_json,
                )
                if sent:
                    return True
                logger.warning("Custom SMTP failed, trying Resend fallback")
        except Exception as e:
            logger.warning(f"Custom SMTP lookup failed: {e}")

    # 2. Try Resend
    if RESEND_API_KEY:
        subject = f"{tipo_nombre} {numero_control} — {emisor_nombre}"
        html_body = _build_dte_html(
            receptor_nombre=receptor_nombre,
            emisor_nombre=emisor_nombre,
            tipo_nombre=tipo_nombre,
            numero_control=numero_control,
            codigo_generacion=codigo_generacion,
            fecha_emision=fecha_emision,
            monto_total=monto_total,
            sello_recepcion=sello_recepcion,
        )
        result = await send_dte_email_resend(
            to_email=to_email,
            subject=subject,
            html_body=html_body,
            pdf_bytes=pdf_bytes,
            pdf_filename=f"{filename_base}.pdf",
            json_bytes=json_bytes_encoded,
            json_filename=f"{filename_base}.json",
        )
        if result["success"]:
            return True
        logger.warning(f"Resend failed: {result.get('error')}, trying GAS fallback")

    # 3. Fallback to GAS legacy
    from app.services.email_service import send_dte_email as send_via_gas
    return await send_via_gas(
        receptor_email=to_email,
        receptor_nombre=receptor_nombre,
        emisor_nombre=emisor_nombre,
        tipo_dte=tipo_dte,
        numero_control=numero_control,
        codigo_generacion=codigo_generacion,
        sello_recepcion=sello_recepcion,
        monto_total=monto_total,
        fecha_emision=fecha_emision,
        pdf_bytes=pdf_bytes,
        dte_json=dte_json,
        org_id=org_id,
    )
