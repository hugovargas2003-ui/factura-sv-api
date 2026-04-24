"""
WhatsApp DTE delivery via Express Engine / WAHA.
Non-blocking — failures don't affect DTE emission.
"""
import httpx
import base64
import re
import logging
import os

logger = logging.getLogger("factura-sv")

EXPRESS_ENGINE_URL = "https://express-engine.algorithmsdata.io/api/v1/dte/send-whatsapp"
EXPRESS_ENGINE_TOKEN = os.getenv(
    "EXPRESS_ENGINE_TOKEN",
    "OnkYA3-yM3xsRM3fX0tcTLpgOPEOsAxK95XoooLzQxE",
)

DTE_TYPE_LABELS = {
    "01": "Factura",
    "03": "Crédito Fiscal",
    "04": "Nota de Remisión",
    "05": "Nota de Crédito",
    "06": "Nota de Débito",
    "07": "Comprobante de Retención",
    "08": "Comprobante de Liquidación",
    "09": "Doc. Contable de Liquidación",
    "11": "Factura de Exportación",
    "14": "Factura Sujeto Excluido",
    "15": "Comprobante de Donación",
}


def normalize_phone(phone: str) -> str:
    """Normalize phone to 503XXXXXXXX (digits only)."""
    if not phone:
        return ""
    digits = re.sub(r"[^\d]", "", phone)
    if digits.startswith("503") and len(digits) >= 11:
        return digits
    if len(digits) == 8:
        return f"503{digits}"
    if len(digits) == 7:
        return f"503{digits}"
    return digits


async def send_dte_whatsapp(
    phone: str,
    pdf_bytes: bytes,
    tipo_dte: str,
    numero_control: str,
    monto_total: float,
    receptor_nombre: str,
    emisor_nombre: str,
    org_id: str,
    dte_id: str,
    fecha_emision: str = "",
    send_json: bool = False,
    json_bytes: bytes = None,
) -> dict:
    """Send DTE PDF via WhatsApp through Express Engine."""
    normalized_phone = normalize_phone(phone)
    if not normalized_phone:
        logger.warning(f"WhatsApp skip: no phone for DTE {numero_control}")
        return {"success": False, "error": "No phone number"}

    filename = f"{tipo_dte}_{numero_control}_{fecha_emision}.pdf".replace("/", "_")

    tipo_label = DTE_TYPE_LABELS.get(tipo_dte, f"DTE Tipo {tipo_dte}")
    caption = (
        f"📄 {tipo_label} | Control: {numero_control} | "
        f"Total: ${monto_total:,.2f} | {emisor_nombre}"
    )

    pdf_base64 = base64.b64encode(pdf_bytes).decode("utf-8")

    payload = {
        "phone": normalized_phone,
        "pdf_base64": pdf_base64,
        "filename": filename,
        "caption": caption,
        "tipo_dte": tipo_dte,
        "numero_control": numero_control,
        "monto": monto_total,
        "receptor_nombre": receptor_nombre,
        "org_id": org_id,
        "dte_id": dte_id,
        "send_json": send_json,
    }

    if send_json and json_bytes:
        payload["json_base64"] = base64.b64encode(json_bytes).decode("utf-8")
        payload["json_filename"] = filename.replace(".pdf", ".json")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                EXPRESS_ENGINE_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {EXPRESS_ENGINE_TOKEN}",
                    "Content-Type": "application/json",
                },
            )

        if response.status_code == 200:
            result = response.json()
            logger.info(
                f"WhatsApp sent: DTE {numero_control} → {normalized_phone} "
                f"(msg_id: {result.get('message_id', '?')})"
            )
            return {"success": True, **result}
        else:
            try:
                error = response.json().get("error", response.text[:200])
            except Exception:
                error = response.text[:200]
            logger.warning(
                f"WhatsApp failed: DTE {numero_control} → {normalized_phone} "
                f"HTTP {response.status_code}: {error}"
            )
            return {"success": False, "error": error, "status_code": response.status_code}

    except httpx.TimeoutException:
        logger.warning(f"WhatsApp timeout: DTE {numero_control} → {normalized_phone}")
        return {"success": False, "error": "Timeout connecting to Express Engine"}
    except Exception as e:
        logger.error(f"WhatsApp error: DTE {numero_control} → {normalized_phone}: {e}")
        return {"success": False, "error": str(e)}
