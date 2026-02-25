"""
FACTURA-SV — Servicio de envío de DTEs por correo electrónico
=============================================================

Integra con Google Apps Script para enviar emails desde contacto@algoritmos.io.

CONFIGURACIÓN:
1. Deployer el Google Apps Script como Web App (ver google_apps_script_dte.js)
2. Agregar estas variables de entorno en Railway:
   - DTE_EMAIL_WEBHOOK_URL=https://script.google.com/macros/s/XXXXXXX/exec
   - DTE_EMAIL_API_KEY=tu_api_key_secreto (mismo que en el Google Apps Script)

USO:
    from app.routers.email_router import DTEEmailService
    
    service = DTEEmailService()
    result = await service.send_dte_email(
        dte_json=dte_data,           # Dict completo del DTE (JSON del MH)
        pdf_bytes=pdf_content,        # bytes del PDF generado
        sello_recibido="2026...",     # Sello del MH
    )
"""

import os
import json
import base64
import logging
import httpx
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger("factura-sv.email")

# ============================================================
# Mapeo de tipos de DTE
# ============================================================
DTE_TYPES = {
    "01": "Factura",
    "03": "Comprobante de Crédito Fiscal",
    "04": "Nota de Remisión",
    "05": "Nota de Crédito",
    "06": "Nota de Débito",
    "07": "Comprobante de Retención",
    "08": "Comprobante de Liquidación",
    "11": "Factura de Exportación",
    "14": "Factura de Sujeto Excluido",
    "15": "Donación",
}

CONDICION_OPERACION = {
    1: "Contado",
    2: "Crédito",
    3: "Otro",
}


class DTEEmailService:
    """
    Servicio para enviar DTEs por correo electrónico usando Google Apps Script.
    
    Envía desde: "DTE FACTURA-SV" <contacto@algoritmos.io>
    Adjuntos: PDF del DTE + JSON firmado del DTE
    """
    
    def __init__(self):
        self.webhook_url = os.getenv("DTE_EMAIL_WEBHOOK_URL", "")
        self.api_key = os.getenv("DTE_EMAIL_API_KEY", "")
        
        if not self.webhook_url:
            logger.warning(
                "DTE_EMAIL_WEBHOOK_URL no configurada. "
                "El envío de emails estará deshabilitado."
            )
        if not self.api_key:
            logger.warning(
                "DTE_EMAIL_API_KEY no configurada. "
                "El envío de emails fallará por autenticación."
            )
    
    @property
    def is_configured(self) -> bool:
        """Verifica si el servicio está correctamente configurado."""
        return bool(self.webhook_url and self.api_key)
    
    async def send_dte_email(
        self,
        dte_json: dict,
        pdf_bytes: Optional[bytes] = None,
        sello_recibido: Optional[str] = None,
        override_email: Optional[str] = None,
    ) -> dict:
        """
        Envía un DTE por correo electrónico al receptor.
        
        Args:
            dte_json: Dict completo del DTE (estructura estándar MH).
            pdf_bytes: Bytes del PDF generado del DTE.
            sello_recibido: Sello de recepción del MH (si no está en dte_json).
            override_email: Email alternativo (sobreescribe el del receptor).
        
        Returns:
            dict con {success: bool, message: str, codigo_generacion: str}
        """
        if not self.is_configured:
            logger.info("Servicio de email no configurado. Omitiendo envío.")
            return {
                "success": False,
                "message": "Servicio de email no configurado",
                "codigo_generacion": dte_json.get("identificacion", {}).get("codigoGeneracion", ""),
            }
        
        try:
            # Extraer datos del DTE JSON
            identificacion = dte_json.get("identificacion", {})
            emisor = dte_json.get("emisor", {})
            receptor = dte_json.get("receptor", {})
            resumen = dte_json.get("resumen", {})
            cuerpo = dte_json.get("cuerpoDocumento", [])
            
            # Determinar email del receptor
            receptor_email = override_email or receptor.get("correo", "")
            if not receptor_email:
                return {
                    "success": False,
                    "message": "El receptor no tiene correo electrónico registrado",
                    "codigo_generacion": identificacion.get("codigoGeneracion", ""),
                }
            
            # Determinar sello (puede venir en diferentes campos según el emisor)
            sello = sello_recibido
            if not sello:
                resp_mh = (
                    dte_json.get("respuestaHacienda", {}) or
                    dte_json.get("responseMH", {}) or
                    {}
                )
                sello = resp_mh.get("selloRecibido", "")
            
            # Preparar resumen de items (máximo 10 para el email)
            items_resumen = []
            for item in cuerpo[:10]:
                items_resumen.append({
                    "descripcion": item.get("descripcion", ""),
                    "cantidad": item.get("cantidad", 1),
                    "precio": item.get("precioUni", 0),
                    "gravada": item.get("ventaGravada", 0),
                })
            
            # Código de generación y número de control
            codigo_generacion = identificacion.get("codigoGeneracion", "")
            numero_control = identificacion.get("numeroControl", "")
            tipo_dte = identificacion.get("tipoDte", "01")
            
            # Determinar IVA total
            total_iva = resumen.get("totalIva", 0)
            if not total_iva:
                tributos = resumen.get("tributos", []) or []
                for tributo in tributos:
                    if isinstance(tributo, dict) and tributo.get("codigo") == "20":
                        total_iva = tributo.get("valor", 0)
                        break
            
            # Preparar nombres de archivos
            pdf_filename = f"{numero_control}_{codigo_generacion}.pdf"
            json_filename = f"{numero_control}_{codigo_generacion}.json"
            
            # Preparar payload para Google Apps Script
            payload = {
                "api_key": self.api_key,
                "receptor_email": receptor_email,
                "receptor_nombre": receptor.get("nombre", ""),
                "emisor_nombre": emisor.get("nombreComercial") or emisor.get("nombre", ""),
                "emisor_nit": emisor.get("nit", ""),
                "tipo_dte": tipo_dte,
                "numero_control": numero_control,
                "codigo_generacion": codigo_generacion,
                "sello_recibido": sello,
                "fecha_emision": identificacion.get("fecEmi", ""),
                "hora_emision": identificacion.get("horEmi", ""),
                "total_pagar": resumen.get("totalPagar", 0),
                "total_letras": resumen.get("totalLetras", ""),
                "total_gravada": resumen.get("totalGravada", 0),
                "total_iva": total_iva,
                "moneda": identificacion.get("tipoMoneda", "USD"),
                "condicion_operacion": resumen.get("condicionOperacion", 1),
                "items_resumen": items_resumen,
                "pdf_filename": pdf_filename,
                "json_filename": json_filename,
            }
            
            # Codificar adjuntos en base64
            if pdf_bytes:
                payload["pdf_base64"] = base64.b64encode(pdf_bytes).decode("utf-8")
            else:
                payload["pdf_base64"] = ""
            
            # JSON del DTE completo como adjunto
            dte_json_str = json.dumps(dte_json, indent=2, ensure_ascii=False)
            payload["json_base64"] = base64.b64encode(
                dte_json_str.encode("utf-8")
            ).decode("utf-8")
            
            # Enviar POST al Google Apps Script
            logger.info(
                f"Enviando DTE email: {DTE_TYPES.get(tipo_dte, tipo_dte)} "
                f"({codigo_generacion}) → {receptor_email}"
            )
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    follow_redirects=True,
                )
            
            # Parsear respuesta
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    logger.info(
                        f"DTE email enviado exitosamente: {codigo_generacion} → {receptor_email}"
                    )
                    return result
                else:
                    error_msg = result.get("error", "Error desconocido")
                    logger.error(f"Google Apps Script error: {error_msg}")
                    return {
                        "success": False,
                        "message": f"Error del servicio de email: {error_msg}",
                        "codigo_generacion": codigo_generacion,
                    }
            else:
                logger.error(
                    f"HTTP error {response.status_code}: {response.text[:200]}"
                )
                return {
                    "success": False,
                    "message": f"HTTP error {response.status_code}",
                    "codigo_generacion": codigo_generacion,
                }
                
        except httpx.TimeoutException:
            logger.error(f"Timeout enviando email para DTE {codigo_generacion}")
            return {
                "success": False,
                "message": "Timeout conectando con servicio de email",
                "codigo_generacion": codigo_generacion,
            }
        except Exception as e:
            logger.error(f"Error inesperado enviando DTE email: {str(e)}")
            return {
                "success": False,
                "message": f"Error inesperado: {str(e)}",
                "codigo_generacion": codigo_generacion,
            }
    
    async def send_test_email(self, to_email: str) -> dict:
        """Envía un email de prueba para verificar la configuración."""
        test_dte = {
            "identificacion": {
                "version": 1,
                "ambiente": "00",
                "tipoDte": "01",
                "numeroControl": "DTE-01-TEST-000000000000001",
                "codigoGeneracion": "TEST-0000-0000-0000-000000000001",
                "fecEmi": "2026-02-24",
                "horEmi": "12:00:00",
                "tipoMoneda": "USD",
            },
            "emisor": {
                "nit": "0000000000000",
                "nrc": "0000000",
                "nombre": "EMPRESA DE PRUEBA",
                "nombreComercial": "EMPRESA DE PRUEBA",
                "correo": "contacto@algoritmos.io",
            },
            "receptor": {
                "nombre": "RECEPTOR DE PRUEBA",
                "correo": to_email,
            },
            "cuerpoDocumento": [
                {
                    "numItem": 1,
                    "descripcion": "PRODUCTO DE PRUEBA",
                    "cantidad": 1,
                    "precioUni": 10.00,
                    "ventaGravada": 10.00,
                }
            ],
            "resumen": {
                "totalGravada": 10.00,
                "totalPagar": 10.00,
                "totalIva": 1.30,
                "totalLetras": "DIEZ 00/100 DOLARES",
                "condicionOperacion": 1,
            },
        }
        
        return await self.send_dte_email(
            dte_json=test_dte,
            pdf_bytes=None,
            sello_recibido="TEST-SELLO-NO-VALIDO",
            override_email=to_email,
        )


# ============================================================
# FastAPI Router — prefix="/email" (main.py agrega /api/v1)
# ============================================================

router = APIRouter(prefix="/email", tags=["Email DTE"])

# Instancia global del servicio
_email_service = None

def get_email_service() -> DTEEmailService:
    """Singleton del servicio de email."""
    global _email_service
    if _email_service is None:
        _email_service = DTEEmailService()
    return _email_service


class TestEmailRequest(BaseModel):
    email: str


class EmailStatusResponse(BaseModel):
    configured: bool
    webhook_url_set: bool
    api_key_set: bool
    sender: str = "contacto@algoritmos.io"
    display_name: str = "DTE FACTURA-SV"
    daily_limit: int = 2000


@router.get("/health")
async def email_health():
    """Health check del servicio de email."""
    service = get_email_service()
    return {
        "status": "ok",
        "configured": service.is_configured,
        "sender": "contacto@algoritmos.io",
    }


@router.get("/status", response_model=EmailStatusResponse)
async def email_status():
    """Verifica el estado de configuración del servicio de email."""
    service = get_email_service()
    return EmailStatusResponse(
        configured=service.is_configured,
        webhook_url_set=bool(service.webhook_url),
        api_key_set=bool(service.api_key),
    )


@router.post("/test")
async def send_test_email(request: TestEmailRequest):
    """
    Envía un email de prueba para verificar la configuración.
    
    El email se envía desde "DTE FACTURA-SV" <contacto@algoritmos.io>
    con un DTE ficticio de prueba.
    """
    service = get_email_service()
    if not service.is_configured:
        raise HTTPException(
            status_code=503,
            detail="Servicio de email no configurado. "
                   "Verificar variables DTE_EMAIL_WEBHOOK_URL y DTE_EMAIL_API_KEY."
        )
    
    result = await service.send_test_email(request.email)
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message"))
    
    return result


# ============================================================
# Función helper para integrar con el flujo de emisión DTE
# ============================================================
async def notify_dte_by_email(
    dte_json: dict,
    pdf_bytes: Optional[bytes] = None,
    sello_recibido: Optional[str] = None,
) -> dict:
    """
    Función helper para llamar después de una emisión DTE exitosa.
    
    Uso típico en el endpoint de emisión:
    
        from app.routers.email_router import notify_dte_by_email
        
        email_result = await notify_dte_by_email(
            dte_json=dte_data,
            pdf_bytes=generated_pdf,
            sello_recibido=mh_response["selloRecibido"],
        )
        # email_result es informativo, no bloquea la emisión
    
    Returns:
        dict con resultado del envío (nunca lanza excepción).
    """
    try:
        service = get_email_service()
        return await service.send_dte_email(
            dte_json=dte_json,
            pdf_bytes=pdf_bytes,
            sello_recibido=sello_recibido,
        )
    except Exception as e:
        logger.error(f"Error no-bloqueante en notify_dte_by_email: {str(e)}")
        return {
            "success": False,
            "message": f"Error no-bloqueante: {str(e)}",
            "codigo_generacion": dte_json.get("identificacion", {}).get("codigoGeneracion", ""),
        }
