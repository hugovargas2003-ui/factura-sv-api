"""
FACTURA-SV: Servicio de Email via Google Script
================================================
Envía DTE (PDF + JSON) al receptor automáticamente tras emisión.
"""
import base64
import json
import logging
import httpx

logger = logging.getLogger(__name__)

GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbw5CyNlSex8xL2vJBxSjg4DOCjwzUkQgiUwgJPO1L7t9H4Z8ZCJ3glCP6chJ4Vtru6ADg/exec"

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
    """Envía PDF + JSON del DTE al receptor por email."""
    if not receptor_email:
        logger.warning(f"DTE {codigo_generacion[:8]}: sin email de receptor, no se envía")
        return False

    nombre_dte = DTE_NOMBRES.get(tipo_dte, f"DTE Tipo {tipo_dte}")
    subject = f"{nombre_dte} #{numero_control[-6:]} — {emisor_nombre}"

    html = _build_html(
        nombre_dte=nombre_dte,
        emisor_nombre=emisor_nombre,
        receptor_nombre=receptor_nombre,
        numero_control=numero_control,
        codigo_generacion=codigo_generacion,
        sello_recepcion=sello_recepcion,
        monto_total=monto_total,
        fecha_emision=fecha_emision,
    )

    # Prepare JSON file
    json_bytes = json.dumps(dte_json, ensure_ascii=False, indent=2).encode("utf-8")
    filename_base = f"DTE-{tipo_dte}-{numero_control.replace('/', '-')}"

    payload = {
        "to": receptor_email,
        "subject": subject,
        "html": html,
        "attachments": [
            {
                "filename": f"{filename_base}.pdf",
                "mimeType": "application/pdf",
                "content": base64.b64encode(pdf_bytes).decode("ascii"),
            },
            {
                "filename": f"{filename_base}.json",
                "mimeType": "application/json",
                "content": base64.b64encode(json_bytes).decode("ascii"),
            },
        ],
    }

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.post(GOOGLE_SCRIPT_URL, json=payload)
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


def _build_html(
    nombre_dte: str, emisor_nombre: str, receptor_nombre: str,
    numero_control: str, codigo_generacion: str, sello_recepcion: str,
    monto_total: float, fecha_emision: str,
) -> str:
    return f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
        <div style="background:#1a3c5e;padding:20px;border-radius:8px 8px 0 0;text-align:center">
            <h1 style="color:#fff;margin:0;font-size:20px">DOCUMENTO TRIBUTARIO ELECTRÓNICO</h1>
            <p style="color:#8bb8e8;margin:5px 0 0;font-size:14px">{nombre_dte}</p>
        </div>

        <div style="background:#f8f9fa;padding:20px;border:1px solid #e0e0e0">
            <p style="margin:0 0 15px;color:#333">Estimado/a <strong>{receptor_nombre}</strong>,</p>
            <p style="margin:0 0 15px;color:#555;font-size:14px">
                Le informamos que <strong>{emisor_nombre}</strong> ha emitido el siguiente
                documento tributario electrónico a su nombre:
            </p>

            <table style="width:100%;border-collapse:collapse;margin:15px 0">
                <tr style="background:#e8f0f8">
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Tipo</strong></td>
                    <td style="padding:8px 12px;font-size:13px;border:1px solid #d0d0d0">{nombre_dte}</td>
                </tr>
                <tr>
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Nº Control</strong></td>
                    <td style="padding:8px 12px;font-size:13px;font-family:monospace;border:1px solid #d0d0d0">{numero_control}</td>
                </tr>
                <tr style="background:#e8f0f8">
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Código</strong></td>
                    <td style="padding:8px 12px;font-size:13px;font-family:monospace;border:1px solid #d0d0d0">{codigo_generacion}</td>
                </tr>
                <tr>
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Fecha</strong></td>
                    <td style="padding:8px 12px;font-size:13px;border:1px solid #d0d0d0">{fecha_emision}</td>
                </tr>
                <tr style="background:#e8f0f8">
                    <td style="padding:8px 12px;font-size:13px;color:#555;border:1px solid #d0d0d0"><strong>Total</strong></td>
                    <td style="padding:8px 12px;font-size:15px;font-weight:bold;color:#1a3c5e;border:1px solid #d0d0d0">${monto_total:,.2f}</td>
                </tr>
            </table>

            <div style="background:#e8f8e8;padding:10px;border-radius:4px;margin:15px 0;border-left:4px solid #27ae60">
                <p style="margin:0;font-size:12px;color:#27ae60">
                    <strong>✅ Validado por el Ministerio de Hacienda</strong><br>
                    Sello: {sello_recepcion}
                </p>
            </div>

            <p style="font-size:13px;color:#555;margin:15px 0 5px">
                <strong>Archivos adjuntos:</strong>
            </p>
            <ul style="font-size:13px;color:#555;margin:0;padding-left:20px">
                <li>📄 <strong>PDF</strong> — Representación gráfica del documento</li>
                <li>📋 <strong>JSON</strong> — Documento tributario electrónico (formato oficial MH)</li>
            </ul>

            <p style="font-size:12px;color:#888;margin:20px 0 5px">
                Puede verificar la autenticidad de este documento en:
                <a href="https://admin.factura.gob.sv/consultaPublica" style="color:#2b6cb0">
                    admin.factura.gob.sv/consultaPublica
                </a>
            </p>
        </div>

        <div style="background:#1a3c5e;padding:12px;border-radius:0 0 8px 8px;text-align:center">
            <p style="color:#8bb8e8;margin:0;font-size:11px">
                Generado por FACTURA-SV | Efficient AI Algorithms LLC | algoritmos.io
            </p>
        </div>
    </div>
    """
