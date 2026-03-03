"""
FACTURA-SV: Servicio de Email via Google Apps Script
=====================================================
Envía DTE (PDF + JSON) al receptor automáticamente tras emisión.
Usa el GAS elaborado que genera HTML profesional con QR, items, totales.
"""
import base64
import json
import logging
import os
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
) -> bool:
    """Envía PDF + JSON del DTE al receptor via Google Apps Script."""
    if not receptor_email:
        logger.warning(f"DTE {codigo_generacion[:8]}: sin email de receptor")
        return False

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
