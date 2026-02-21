"""
FACTURA-SV — Module 3: TransmitService
Handles transmission of signed DTEs to the MH API.

Flow:
1. DTE is already signed as JWT (from SignEngine)
2. Build the `peticion` payload expected by MH
3. POST to /fesv/recepciondte with Bearer token
4. Parse response: sello_recepcion (accepted) or observaciones (rejected)

MH Recepción Endpoint:
- URL: POST /fesv/recepciondte
- Headers: Authorization: Bearer {token}, Content-Type: application/json
- Body: {
    "ambiente": "00"|"01",
    "idEnvio": 1,
    "version": 1|2|3,
    "tipoDte": "01"|"03"|...,
    "documento": "<signed JWT string>",
    "codigoGeneracion": "<UUID v4>"
  }
- Response (success): {
    "version": 1,
    "ambiente": "01",
    "versionApp": 1,
    "estado": "PROCESADO",
    "codigoGeneracion": "...",
    "selloRecibido": "...",
    "fhProcesamiento": "...",
    "clasificaMsg": "...",
    "codigoMsg": "...",
    "descripcionMsg": "...",
    "observaciones": []
  }
"""

import httpx
import logging
import uuid
from datetime import datetime, timezone

from app.core.config import get_mh_url, settings, MHEnvironment
from app.modules.auth_bridge import TokenInfo
from app.schemas.models import TransmitResponse

logger = logging.getLogger(__name__)

# DTE version mapping — each DTE type has its own JSON schema version
DTE_SCHEMA_VERSIONS: dict[str, int] = {
    "01": 1,   # Factura
    "03": 3,   # CCF
    "04": 1,   # Nota de Remisión
    "05": 3,   # Nota de Crédito
    "06": 3,   # Nota de Débito
    "07": 3,   # Comprobante de Retención
    "08": 1,   # Comprobante de Liquidación
    "09": 1,   # Documento Contable de Liquidación
    "11": 1,   # Factura de Exportación
    "14": 1,   # Factura de Sujeto Excluido
    "15": 1,   # Comprobante de Donación
}


class TransmitError(Exception):
    """Raised when DTE transmission fails."""
    def __init__(self, message: str, status_code: int = 500,
                 mh_response: dict = None, observaciones: list = None):
        self.message = message
        self.status_code = status_code
        self.mh_response = mh_response or {}
        self.observaciones = observaciones or []
        super().__init__(self.message)


class TransmitService:
    """
    Manages DTE transmission to the MH.

    Usage:
        service = TransmitService()
        result = await service.transmit(
            token_info=token,
            signed_dte=jwt_string,
            tipo_dte="03",
            codigo_generacion="UUID-...",
        )
    """

    # Retry config
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 2
    TIMEOUT_SECONDS = 60  # MH can be slow

    async def transmit(
        self,
        token_info: TokenInfo,
        signed_dte: str,
        tipo_dte: str,
        codigo_generacion: str,
        id_envio: int = 1,
    ) -> TransmitResponse:
        """
        Transmit a signed DTE to the MH.

        Args:
            token_info: Valid authentication token from AuthBridge
            signed_dte: JWT string (signed DTE from SignEngine)
            tipo_dte: DTE type code ("01", "03", etc.)
            codigo_generacion: UUID v4 of the DTE
            id_envio: Transmission ID (default 1 for single document)

        Returns:
            TransmitResponse with sello_recepcion or observaciones

        Raises:
            TransmitError: If transmission fails
        """
        # Use token's environment (billing tokens are PRODUCTION even if global is TEST)
        token_env = getattr(token_info, 'environment', settings.mh_environment)
        from app.core.config import MH_URLS
        url = MH_URLS[token_env]["recepcion_dte"]

        # Determine ambiente code
        ambiente = "00" if token_env == MHEnvironment.TEST else "01"

        # Get schema version for this DTE type
        version = DTE_SCHEMA_VERSIONS.get(tipo_dte, 1)

        # Build the peticion payload as MH expects it
        payload = {
            "ambiente": ambiente,
            "idEnvio": id_envio,
            "version": version,
            "tipoDte": tipo_dte,
            "documento": signed_dte,
            "codigoGeneracion": codigo_generacion,
        }

        headers = {
            "Authorization": token_info.bearer,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        logger.info(
            f"Transmitting DTE: type={tipo_dte}, "
            f"codGen={codigo_generacion[:8]}..., "
            f"env={ambiente}, version={version}"
        )

        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=self.TIMEOUT_SECONDS, verify=True
                ) as client:
                    response = await client.post(url, json=payload, headers=headers)

                return self._parse_response(response, codigo_generacion)

            except TransmitError:
                raise  # Don't retry business logic errors

            except httpx.TimeoutException:
                last_error = TransmitError(
                    message=f"Timeout en transmisión al MH (intento {attempt}/{self.MAX_RETRIES}). "
                            f"El MH no respondió en {self.TIMEOUT_SECONDS}s.",
                    status_code=504,
                )
                logger.warning(f"Timeout on attempt {attempt}/{self.MAX_RETRIES}")

            except httpx.ConnectError as e:
                last_error = TransmitError(
                    message=f"No se pudo conectar con el MH (intento {attempt}/{self.MAX_RETRIES}).",
                    status_code=502,
                )
                logger.warning(f"Connection error on attempt {attempt}: {e}")

            except Exception as e:
                last_error = TransmitError(
                    message=f"Error inesperado en transmisión: {str(e)}",
                    status_code=500,
                )
                logger.exception(f"Unexpected error on attempt {attempt}: {e}")

            # Wait before retry (exponential backoff with cap)
            if attempt < self.MAX_RETRIES:
                import asyncio
                delay = min(60, 2 ** attempt)  # 2s, 4s, 8s... capped at 60s
                logger.info(f"Retrying in {delay}s...")
                await asyncio.sleep(delay)

        # All retries exhausted
        raise last_error

    def _parse_response(self, response: httpx.Response, codigo_generacion: str) -> TransmitResponse:
        """Parse the MH response and return structured result."""
        # Safely attempt JSON parsing — MH may return HTML on 500/502/503
        try:
            data = response.json()
        except Exception:
            raise TransmitError(
                message=(
                    f"El MH retornó una respuesta no-JSON (HTTP {response.status_code}). "
                    f"Esto suele indicar un error interno del MH o mantenimiento. "
                    f"Respuesta: {response.text[:300]}"
                ),
                status_code=response.status_code,
            )

        if response.status_code == 200:
            estado = data.get("estado", "DESCONOCIDO")
            observaciones = data.get("observaciones", [])

            # Flatten observaciones if they're nested
            obs_flat = []
            if isinstance(observaciones, list):
                for obs in observaciones:
                    if isinstance(obs, str):
                        obs_flat.append(obs)
                    elif isinstance(obs, dict):
                        obs_flat.extend(obs.values())
                    elif isinstance(obs, list):
                        obs_flat.extend([str(o) for o in obs])

            if estado == "PROCESADO":
                logger.info(
                    f"DTE ACCEPTED: codGen={codigo_generacion[:8]}..., "
                    f"sello={data.get('selloRecibido', 'N/A')[:16]}..."
                )
            else:
                logger.warning(
                    f"DTE response estado={estado}: codGen={codigo_generacion[:8]}..., "
                    f"obs={obs_flat}"
                )

            return TransmitResponse(
                status=estado,
                sello_recepcion=data.get("selloRecibido"),
                codigo_generacion=data.get("codigoGeneracion", codigo_generacion),
                numero_control=None,  # Not in MH response; it's in the DTE itself
                fecha_procesamiento=data.get("fhProcesamiento"),
                observaciones=obs_flat,
                clasificacion_msg=data.get("clasificaMsg"),
                codigo_msg=data.get("codigoMsg"),
                descripcion_msg=data.get("descripcionMsg"),
                raw_response=data,
            )

        elif response.status_code == 401:
            raise TransmitError(
                message="Token de autenticación inválido o expirado. Re-autentique con el MH.",
                status_code=401,
                mh_response=data,
            )

        elif response.status_code == 400:
            observaciones = data.get("observaciones", [])
            raise TransmitError(
                message=f"El MH rechazó el DTE: {data.get('descripcionMsg', 'Error de validación')}",
                status_code=400,
                mh_response=data,
                observaciones=observaciones if isinstance(observaciones, list) else [str(observaciones)],
            )

        else:
            raise TransmitError(
                message=f"Error del MH (HTTP {response.status_code}): "
                        f"{data.get('descripcionMsg', response.text[:300])}",
                status_code=response.status_code,
                mh_response=data,
            )


# Singleton instance
transmit_service = TransmitService()
