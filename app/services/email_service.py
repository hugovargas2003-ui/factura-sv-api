"""
FACTURA-SV: Servicio de Email via Google Apps Script
=====================================================
Envía DTE (PDF + JSON) al receptor automáticamente tras emisión.
Usa el GAS elaborado que genera HTML profesional con QR, items, totales.

Custom SMTP: Si la org tiene SMTP propio configurado y verificado,
se envía desde su correo. Si no, fallback a GAS de plataforma.
"""
import base64
import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import httpx

logger = logging.getLogger(__name__)

# URL y API Key del Google Apps Script (variables de entorno en Railway)
GAS_URL = os.getenv(
    "DTE_EMAIL_WEBHOOK_URL",
    os.getenv("EMAIL_SCRIPT_URL", ""),
)
GAS_API_KEY = os.getenv("DTE_EMAIL_API_KEY", "")

DTE_NOMBRES = {
    "01": "Factura", "03": "Comprobante de Crédito Fiscal",
    "04": "Nota de Remisión", "05": "Nota de Crédito",
    "06": "Nota de Débito", "07": "Comprobante de Retención",
    "08": "Comprobante de Liquidación", "09": "Doc. Contable de Liquidación",
    "11": "Factura de Exportación", "14": "Factura de Sujeto Excluido",
    "15": "Comprobante de Donación",
}


async def _send_via_custom_smtp(
    smtp_config: dict,
    receptor_email: str,
    receptor_nombre: str,
    emisor_nombre: str,
    tipo_dte: str,
    numero_control: str,
    codigo_generacion: str,
    monto_total: float,
    fecha_emision: str,
    pdf_bytes: bytes | None,
    dte_json: dict,
) -> bool:
    """Send DTE email using org's custom SMTP server."""
    try:
        from app.services.encryption_service import EncryptionService
        enc = EncryptionService()
        org_id = smtp_config["org_id"]
        password = enc.decrypt_string(
            smtp_config["smtp_password_encrypted"].encode("utf-8"), org_id
        )

        tipo_nombre = DTE_NOMBRES.get(tipo_dte, tipo_dte)
        from_name = smtp_config.get("from_name") or emisor_nombre
        from_email = smtp_config.get("from_email") or smtp_config["smtp_user"]

        msg = MIMEMultipart()
        msg["Subject"] = f"{tipo_nombre} {numero_control} — {emisor_nombre}"
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = receptor_email

        body_text = (
            f"Estimado/a {receptor_nombre},\n\n"
            f"Se adjunta su {tipo_nombre} electrónico.\n\n"
            f"Número de control: {numero_control}\n"
            f"Código de generación: {codigo_generacion}\n"
            f"Fecha: {fecha_emision}\n"
            f"Total: ${monto_total:.2f}\n\n"
            f"Puede verificar este documento en:\n"
            f"https://admin.factura.gob.sv/consultaPublica\n\n"
            f"— {from_name}\n"
            f"Emitido vía FACTURA-SV"
        )
        msg.attach(MIMEText(body_text, "plain", "utf-8"))

        filename_base = f"DTE-{tipo_dte}-{numero_control.replace('/', '-')}"

        if pdf_bytes:
            pdf_part = MIMEBase("application", "pdf")
            pdf_part.set_payload(pdf_bytes)
            encoders.encode_base64(pdf_part)
            pdf_part.add_header("Content-Disposition", "attachment", filename=f"{filename_base}.pdf")
            msg.attach(pdf_part)

        json_bytes = json.dumps(dte_json, ensure_ascii=False, indent=2).encode("utf-8")
        json_part = MIMEBase("application", "json")
        json_part.set_payload(json_bytes)
        encoders.encode_base64(json_part)
        json_part.add_header("Content-Disposition", "attachment", filename=f"{filename_base}.json")
        msg.attach(json_part)

        server = smtplib.SMTP(smtp_config["smtp_host"], smtp_config["smtp_port"], timeout=15)
        if smtp_config.get("use_tls", True):
            server.starttls()
        server.login(smtp_config["smtp_user"], password)
        server.send_message(msg)
        server.quit()

        logger.info(f"Custom SMTP email sent: {receptor_email} | DTE {codigo_generacion[:8]} via {from_email}")
        return True
    except Exception as e:
        logger.error(f"Custom SMTP failed for org {smtp_config.get('org_id', '?')}: {e}")
        return False


async def send_dte_email(
    receptor_email: str,
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
    org_id: str | None = None,
) -> bool:
    """Envía PDF + JSON del DTE al receptor via Google Apps Script o SMTP custom."""
    if not receptor_email:
        logger.warning(f"DTE {codigo_generacion[:8]}: sin email de receptor")
        return False

    # Check for custom SMTP config
    if org_id:
        try:
            from app.dependencies import get_supabase
            db = get_supabase()
            smtp_result = db.table("org_email_config").select("*").eq(
                "org_id", org_id
            ).eq("use_custom_email", True).eq("is_verified", True).limit(1).execute()
            if smtp_result.data:
                return await _send_via_custom_smtp(
                    smtp_config=smtp_result.data[0],
                    receptor_email=receptor_email,
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
        except Exception as e:
            logger.warning(f"Custom SMTP lookup failed, falling back to GAS: {e}")

    # Fallback: platform GAS email
    if not GAS_URL:
        logger.warning("Email no configurado: DTE_EMAIL_WEBHOOK_URL / EMAIL_SCRIPT_URL vacío")
        return False

    # --- Extraer datos completos del dte_json ---
    identificacion = dte_json.get("identificacion", {})
    emisor = dte_json.get("emisor", {})
    receptor = dte_json.get("receptor", {})
    resumen = dte_json.get("resumen", {})
    cuerpo = dte_json.get("cuerpoDocumento", [])
    ambiente = identificacion.get("ambiente", "01")

    # IVA total (varía según tipo DTE)
    total_iva = 0
    if tipo_dte == "03":
        total_iva = resumen.get("totalIva", resumen.get("ivaPerci1", 0)) or 0
    elif tipo_dte == "01":
        gravada = resumen.get("totalGravada", 0) or 0
        total_iva = round(gravada - gravada / 1.13, 2) if gravada else 0
    else:
        total_iva = resumen.get("totalIva", 0) or 0

    # Items resumen para la tabla del email
    items_resumen = []
    if isinstance(cuerpo, list):
        for item in cuerpo:
            items_resumen.append({
                "descripcion": item.get("descripcion", ""),
                "cantidad": item.get("cantidad", 1),
                "precio": item.get("precioUni", item.get("montoDescu", 0)),
                "gravada": item.get("ventaGravada", item.get("compra", 0)) or 0,
            })

    # Nombre de archivos
    filename_base = f"DTE-{tipo_dte}-{numero_control.replace('/', '-')}"

    # JSON del DTE como bytes
    json_bytes = json.dumps(dte_json, ensure_ascii=False, indent=2).encode("utf-8")

    # --- Payload en formato que espera el GAS elaborado ---
    payload = {
        "api_key": GAS_API_KEY,
        "receptor_email": receptor_email,
        "receptor_nombre": receptor_nombre,
        "emisor_nombre": emisor_nombre,
        "emisor_nit": emisor.get("nit", ""),
        "tipo_dte": tipo_dte,
        "numero_control": numero_control,
        "codigo_generacion": codigo_generacion,
        "sello_recibido": sello_recepcion,
        "fecha_emision": fecha_emision,
        "hora_emision": identificacion.get("horEmi", ""),
        "total_pagar": resumen.get("totalPagar", monto_total) or monto_total,
        "total_letras": resumen.get("totalLetras", ""),
        "total_gravada": resumen.get("totalGravada", 0) or 0,
        "total_iva": total_iva,
        "moneda": identificacion.get("tipoMoneda", "USD"),
        "condicion_operacion": resumen.get("condicionOperacion", 1),
        "items_resumen": items_resumen,
        "ambiente": ambiente,
        "pdf_base64": base64.b64encode(pdf_bytes).decode("ascii") if pdf_bytes else "",
        "json_base64": base64.b64encode(json_bytes).decode("ascii"),
        "pdf_filename": f"{filename_base}.pdf",
        "json_filename": f"{filename_base}.json",
    }

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.post(
                GAS_URL,
                content=json.dumps(payload),
                headers={"Content-Type": "text/plain"},
                follow_redirects=True,
            )
            result = resp.json()
            if result.get("success"):
                logger.info(f"✅ Email enviado: {receptor_email} | DTE {codigo_generacion[:8]}")
                return True
            else:
                logger.error(f"❌ Email falló: {result.get('error')}")
                return False
    except Exception as e:
        logger.error(f"❌ Error enviando email: {e}")
        return False
