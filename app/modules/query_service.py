"""
FACTURA-SV — Module 4: QueryService
Query DTE status and details from the MH.

MH Consulta Endpoint:
- URL: POST /fesv/recepcion/consultadte/
- Headers: Authorization: Bearer {token}, Content-Type: application/json
- Body: {
    "nitEmisor": "0614-...",
    "tdte": "01"|"03"|...,
    "codigoGeneracion": "UUID-..."
  }
"""

import httpx
import logging

from app.core.config import get_mh_url
from app.modules.auth_bridge import TokenInfo
from app.schemas.models import QueryResponse

logger = logging.getLogger(__name__)


class QueryError(Exception):
    def __init__(self, message: str, status_code: int = 500, mh_response: dict = None):
        self.message = message
        self.status_code = status_code
        self.mh_response = mh_response or {}
        super().__init__(self.message)


class QueryService:
    """
    Query DTEs from the MH.

    Usage:
        service = QueryService()
        result = await service.query(token, nit, tipo_dte, codigo_generacion)
    """

    TIMEOUT_SECONDS = 30

    async def query(
        self,
        token_info: TokenInfo,
        nit_emisor: str,
        tipo_dte: str,
        codigo_generacion: str,
    ) -> QueryResponse:
        """
        Query a DTE by its código de generación.

        Args:
            token_info: Valid MH auth token
            nit_emisor: NIT of the DTE issuer
            tipo_dte: DTE type code
            codigo_generacion: UUID of the DTE

        Returns:
            QueryResponse with DTE data

        Raises:
            QueryError: If query fails
        """
        url = get_mh_url("consulta_dte")

        payload = {
            "nitEmisor": nit_emisor,
            "tdte": tipo_dte,
            "codigoGeneracion": codigo_generacion,
        }

        headers = {
            "Authorization": token_info.bearer,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        logger.info(f"Querying DTE: type={tipo_dte}, codGen={codigo_generacion[:8]}...")

        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT_SECONDS, verify=True) as client:
                response = await client.post(url, json=payload, headers=headers)

            data = response.json() if response.status_code != 500 else {}

            if response.status_code == 200:
                return QueryResponse(
                    status="found",
                    dte_data=data,
                    sello_recepcion=data.get("selloRecibido"),
                    fecha_procesamiento=data.get("fhProcesamiento"),
                    raw_response=data,
                )

            elif response.status_code == 404 or (
                isinstance(data, dict) and (
                    data.get("estado", "").upper() == "NO ENCONTRADO"
                    or data.get("codigoMsg") in ("004", "005")
                    or data.get("descripcionMsg", "").lower().startswith("no encontrado")
                )
            ):
                return QueryResponse(
                    status="not_found",
                    message=f"DTE no encontrado: tipo={tipo_dte}, codGen={codigo_generacion}",
                    raw_response=data,
                )

            elif response.status_code == 401:
                raise QueryError(
                    "Token expirado o inválido. Re-autentique.",
                    status_code=401, mh_response=data,
                )

            else:
                raise QueryError(
                    f"Error del MH (HTTP {response.status_code}): {data}",
                    status_code=response.status_code, mh_response=data,
                )

        except QueryError:
            raise

        except httpx.TimeoutException:
            raise QueryError("Timeout al consultar el MH.", status_code=504)

        except httpx.ConnectError:
            raise QueryError("No se pudo conectar con el MH.", status_code=502)

        except Exception as e:
            logger.exception(f"Error querying DTE: {e}")
            raise QueryError(f"Error inesperado: {str(e)}", status_code=500) from e


# Singleton
query_service = QueryService()
