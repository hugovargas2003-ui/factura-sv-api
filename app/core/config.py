"""
FACTURA-SV Core Configuration
MH API URLs and application settings.
"""

import os
from enum import Enum

try:
    from pydantic_settings import BaseSettings

    class MHEnvironment(str, Enum):
        TEST = "test"
        PRODUCTION = "production"

    class Settings(BaseSettings):
        app_name: str = "FACTURA-SV"
        app_version: str = "1.0.0"
        debug: bool = True
        mh_environment: MHEnvironment = MHEnvironment.TEST
        host: str = "0.0.0.0"
        port: int = 8000

        class Config:
            env_file = ".env"
            env_file_encoding = "utf-8"

except ImportError:
    # Fallback without pydantic_settings (for testing or minimal installs)
    class MHEnvironment(str, Enum):
        TEST = "test"
        PRODUCTION = "production"

    class Settings:
        def __init__(self):
            self.app_name = os.getenv("APP_NAME", "FACTURA-SV")
            self.app_version = os.getenv("APP_VERSION", "1.0.0")
            self.debug = os.getenv("DEBUG", "true").lower() == "true"
            self.mh_environment = MHEnvironment(os.getenv("MH_ENVIRONMENT", "test"))
            self.host = os.getenv("HOST", "0.0.0.0")
            self.port = int(os.getenv("PORT", "8000"))


settings = Settings()


# ─────────────────────────────────────────────────────────────
# MH API URL REGISTRY
# Source: DGII "Guía de Integración Factura Electrónica SV"
# https://factura.gob.sv/wp-content/uploads/2021/11/
#   FESVDGIIMH_GuiaIntegracionFacturaElectronicasSV.pdf
# ─────────────────────────────────────────────────────────────

MH_URLS = {
    MHEnvironment.TEST: {
        "auth":          "https://apitest.dtes.mh.gob.sv/seguridad/auth",
        "recepcion_dte": "https://apitest.dtes.mh.gob.sv/fesv/recepciondte",
        "consulta_dte":  "https://apitest.dtes.mh.gob.sv/fesv/recepcion/consultadte/",
        "anulacion_dte": "https://apitest.dtes.mh.gob.sv/fesv/anulardte",
        "contingencia":  "https://apitest.dtes.mh.gob.sv/fesv/contingencia",
    },
    MHEnvironment.PRODUCTION: {
        "auth":          "https://api.dtes.mh.gob.sv/seguridad/auth",
        "recepcion_dte": "https://api.dtes.mh.gob.sv/fesv/recepciondte",
        "consulta_dte":  "https://api.dtes.mh.gob.sv/fesv/recepcion/consultadte/",
        "anulacion_dte": "https://api.dtes.mh.gob.sv/fesv/anulardte",
        "contingencia":  "https://api.dtes.mh.gob.sv/fesv/contingencia",
    },
}


def get_mh_url(service: str) -> str:
    """Get the MH API URL for a service based on current environment."""
    env = settings.mh_environment
    urls = MH_URLS.get(env)
    if not urls:
        raise ValueError(f"Unknown MH environment: {env}")
    url = urls.get(service)
    if not url:
        raise ValueError(f"Unknown MH service: {service}")
    return url
