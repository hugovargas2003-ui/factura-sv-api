"""
FACTURA-SV - ContingencyService
Handles contingency event notification to MH (certified v3).
"""
import httpx
import logging
import uuid
from datetime import datetime, timezone
from app.core.config import get_mh_url, settings, MHEnvironment
from app.modules.auth_bridge import TokenInfo
from app.modules.sign_engine import CertificateSession, sign_engine

logger = logging.getLogger(__name__)


class ContingencyError(Exception):
    def __init__(self, message, status_code=500, mh_response=None):
        self.message = message
        self.status_code = status_code
        self.mh_response = mh_response or {}
        super().__init__(self.message)


class ContingencyService:
    TIMEOUT_SECONDS = 60

    def build_contingency_document(self, nit_emisor, nombre_emisor,
            nombre_comercial, cod_establecimiento, cod_punto_venta,
            telefono, correo, motivo, fecha_inicio, hora_inicio,
            fecha_fin, hora_fin, detalle_dte):
        ambiente = "00" if settings.mh_environment == MHEnvironment.TEST else "01"
        return {
            "identificacion": {
                "version": 3, "ambiente": ambiente,
                "codigoGeneracion": str(uuid.uuid4()).upper(),
                "fecInicio": fecha_inicio, "horInicio": hora_inicio,
                "fecFin": fecha_fin, "horFin": hora_fin,
            },
            "emisor": {
                "nit": nit_emisor, "nombre": nombre_emisor,
                "tipoEstablecimiento": "01",
                "nomEstablecimiento": nombre_comercial or nombre_emisor,
                "codEstableMH": cod_establecimiento,
                "codEstable": cod_establecimiento,
                "codPuntoVentaMH": cod_punto_venta,
                "codPuntoVenta": cod_punto_venta,
                "telefono": telefono or "00000000",
                "correo": correo or "",
            },
            "motivo": motivo,
            "detalleDTE": [
                {"tipoDte": d.get("tipo_dte", "01"),
                 "codigoGeneracion": d["codigo_generacion"],
                 "selloRecibido": d.get("sello_recibido"),
                 "numeroControl": d["numero_control"],
                 "fecEmi": d["fecha_emision"],
                 "horEmi": d.get("hora_emision", "00:00:00")}
                for d in detalle_dte
            ],
        }

    async def notify(self, token_info, cert_session, contingency_doc):
        url = get_mh_url("contingencia")
        signed_jws = sign_engine.sign_dte(cert_session, contingency_doc)
        ambiente = "00" if settings.mh_environment == MHEnvironment.TEST else "01"
        payload = {"ambiente": ambiente, "idEnvio": 1, "version": 3, "documento": signed_jws}
        headers = {"Authorization": token_info.bearer, "Content-Type": "application/json"}
        cg = contingency_doc.get("identificacion", {}).get("codigoGeneracion", "?")
        logger.info(f"Sending contingency: codGen={cg[:8]}...")
        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT_SECONDS, verify=True) as client:
                response = await client.post(url, json=payload, headers=headers)
            data = response.json() if response.status_code != 500 else {}
            if response.status_code == 200:
                return {"success": True, "estado": data.get("estado"),
                        "sello_recibido": data.get("selloRecibido"),
                        "descripcion": data.get("descripcionMsg"), "raw_response": data}
            elif response.status_code == 401:
                raise ContingencyError("Token expirado", 401, data)
            elif response.status_code == 400:
                raise ContingencyError(f"MH rechazo: {data.get(chr(39)+'descripcionMsg'+chr(39), str(data))}", 400, data)
            else:
                raise ContingencyError(f"Error MH HTTP {response.status_code}", response.status_code, data)
        except ContingencyError:
            raise
        except httpx.TimeoutException:
            raise ContingencyError("Timeout", 504)
        except httpx.ConnectError:
            raise ContingencyError("No se pudo conectar con MH", 502)
        except Exception as e:
            logger.exception(f"Contingency error: {e}")
            raise ContingencyError(str(e), 500) from e


contingency_service = ContingencyService()
