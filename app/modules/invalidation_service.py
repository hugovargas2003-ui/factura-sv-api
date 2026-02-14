"""
FACTURA-SV — Module 5: InvalidationService
Handles DTE invalidation (anulación) via MH API.

MH Invalidation Endpoint:
- URL: POST /fesv/anulardte
- Headers: Authorization: Bearer {token}, Content-Type: application/json
- Body: Signed invalidation JSON (JWT) with the invalidation document

Invalidation Document Structure:
{
  "identificacion": {
    "version": 2,
    "ambiente": "00"|"01",
    "codigoGeneracion": "<UUID new for this invalidation>",
    "fecAnula": "YYYY-MM-DD",
    "horAnula": "HH:MM:SS"
  },
  "emisor": { "nit": "...", "nombre": "...", "tipoEstablecimiento": "01", "nomEstablecimiento": "...", "telefono": "...", "correo": "..." },
  "documento": {
    "tipoDte": "01"|"03"|...,
    "codigoGeneracion": "<UUID of original DTE>",
    "selloRecibido": "<sello from original transmission>",
    "numeroControl": "<control number of original>",
    "fecEmi": "YYYY-MM-DD",
    "montoIva": 0.00,
    "codigoGeneracionR": null,
    "tipoDocumento": "36"|"13",
    "numDocumento": "NIT or DUI",
    "nombre": "receptor name"
  },
  "motivo": {
    "tipoAnulacion": 1|2|3,
    "motivoAnulacion": "free text",
    "nombreResponsable": "...",
    "tipDocResponsable": "36",
    "numDocResponsable": "NIT",
    "nombreSolicita": "...",
    "tipDocSolicita": "36",
    "numDocSolicita": "NIT"
  }
}
"""

import httpx
import logging
import uuid
from datetime import datetime, timezone

from app.core.config import get_mh_url, settings, MHEnvironment
from app.modules.auth_bridge import TokenInfo
from app.modules.sign_engine import CertificateSession, sign_engine
from app.schemas.models import InvalidateRequest, InvalidateResponse

logger = logging.getLogger(__name__)


class InvalidationError(Exception):
    def __init__(self, message: str, status_code: int = 500,
                 mh_response: dict = None, observaciones: list = None):
        self.message = message
        self.status_code = status_code
        self.mh_response = mh_response or {}
        self.observaciones = observaciones or []
        super().__init__(self.message)


class InvalidationService:
    """
    Manages DTE invalidation with the MH.

    Usage:
        service = InvalidationService()
        result = await service.invalidate(token, cert_session, request_data)
    """

    TIMEOUT_SECONDS = 60

    def build_invalidation_document(self, request: InvalidateRequest) -> dict:
        """
        Build the invalidation JSON document according to MH schema.
        All required fields are now validated in InvalidateRequest.
        This document will be signed and sent to the MH.
        """
        now = datetime.now(timezone.utc)
        ambiente = "00" if settings.mh_environment == MHEnvironment.TEST else "01"

        return {
            "identificacion": {
                "version": 2,
                "ambiente": ambiente,
                "codigoGeneracion": str(uuid.uuid4()).upper(),
                "fecAnula": now.strftime("%Y-%m-%d"),
                "horAnula": now.strftime("%H:%M:%S"),
            },
            "emisor": {
                "nit": request.nit_emisor,
                "nombre": request.nombre_emisor,
                "tipoEstablecimiento": "01",
            },
            "documento": {
                "tipoDte": request.tipo_dte.value,
                "codigoGeneracion": request.codigo_generacion_doc,
                "selloRecibido": request.sello_recibido,
                "numeroControl": request.numero_control,
                "fecEmi": request.fecha_emision,
                "montoIva": request.monto_iva,
                "codigoGeneracionR": None,
                "tipoDocumento": request.tipo_documento_responsable,
                "numDocumento": request.nit_receptor,
                "nombre": request.nombre_receptor,
            },
            "motivo": {
                "tipoAnulacion": int(request.tipo_invalidacion.value),
                "motivoAnulacion": request.motivo,
                "nombreResponsable": request.nombre_responsable,
                "tipDocResponsable": request.tipo_documento_responsable,
                "numDocResponsable": request.num_documento_responsable,
                "nombreSolicita": request.nombre_responsable,
                "tipDocSolicita": request.tipo_documento_responsable,
                "numDocSolicita": request.num_documento_responsable,
            },
        }

    async def invalidate(
        self,
        token_info: TokenInfo,
        cert_session: CertificateSession,
        invalidation_doc: dict,
    ) -> InvalidateResponse:
        """
        Sign and transmit an invalidation document to the MH.

        Args:
            token_info: Valid MH auth token
            cert_session: Active certificate session for signing
            invalidation_doc: Built invalidation document

        Returns:
            InvalidateResponse

        Raises:
            InvalidationError
        """
        url = get_mh_url("anulacion_dte")

        # Sign the invalidation document
        signed_jwt = sign_engine.sign_dte(cert_session, invalidation_doc)

        ambiente = "00" if settings.mh_environment == MHEnvironment.TEST else "01"

        payload = {
            "ambiente": ambiente,
            "idEnvio": 1,
            "version": 2,
            "documento": signed_jwt,
        }

        headers = {
            "Authorization": token_info.bearer,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        codigo_gen = invalidation_doc.get("identificacion", {}).get("codigoGeneracion", "?")
        logger.info(f"Sending invalidation: codGen={codigo_gen[:8]}...")

        try:
            async with httpx.AsyncClient(
                timeout=self.TIMEOUT_SECONDS, verify=True
            ) as client:
                response = await client.post(url, json=payload, headers=headers)

            data = response.json() if response.status_code != 500 else {}

            if response.status_code == 200:
                estado = data.get("estado", "DESCONOCIDO")
                observaciones = data.get("observaciones", [])

                return InvalidateResponse(
                    status=estado,
                    sello_invalidacion=data.get("selloRecibido"),
                    fecha_procesamiento=data.get("fhProcesamiento"),
                    descripcion_msg=data.get("descripcionMsg"),
                    observaciones=observaciones if isinstance(observaciones, list) else [],
                    raw_response=data,
                )

            elif response.status_code == 401:
                raise InvalidationError(
                    "Token expirado. Re-autentique.", status_code=401, mh_response=data,
                )

            elif response.status_code == 400:
                raise InvalidationError(
                    f"MH rechazó la invalidación: {data.get('descripcionMsg', str(data))}",
                    status_code=400, mh_response=data,
                    observaciones=data.get("observaciones", []),
                )

            else:
                raise InvalidationError(
                    f"Error del MH (HTTP {response.status_code})",
                    status_code=response.status_code, mh_response=data,
                )

        except InvalidationError:
            raise
        except httpx.TimeoutException:
            raise InvalidationError("Timeout al invalidar.", status_code=504)
        except httpx.ConnectError:
            raise InvalidationError("No se pudo conectar con el MH.", status_code=502)
        except Exception as e:
            logger.exception(f"Error in invalidation: {e}")
            raise InvalidationError(f"Error inesperado: {str(e)}", status_code=500) from e


# Singleton
invalidation_service = InvalidationService()
