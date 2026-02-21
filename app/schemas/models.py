"""
FACTURA-SV Pydantic Schemas
Request/response models for the API.
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Any
from enum import Enum


# ─────────────────────────────────────────────────────────────
# ENUMS
# ─────────────────────────────────────────────────────────────

class TipoDTE(str, Enum):
    FACTURA = "01"
    CCF = "03"
    NOTA_REMISION = "04"
    NOTA_CREDITO = "05"
    NOTA_DEBITO = "06"
    RETENCION = "07"
    LIQUIDACION = "08"
    DOC_CONTABLE_LIQ = "09"
    EXPORTACION = "11"
    SUJETO_EXCLUIDO = "14"
    DONACION = "15"


class AmbienteMH(str, Enum):
    TEST = "00"
    PRODUCTION = "01"


class ModeloTransmision(str, Enum):
    NORMAL = "1"       # Transmisión normal (síncrono, uno a uno)
    CONTINGENCIA = "2"  # Transmisión por contingencia (lote)


class TipoTransmision(str, Enum):
    NORMAL = "1"
    CONTINGENCIA = "2"


# ─────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────

class AuthRequest(BaseModel):
    """Credentials for MH Oficina Virtual authentication."""
    nit: str = Field(..., description="NIT del contribuyente (formato: 0614-XXXXXX-XXX-X)")
    password: str = Field(..., description="Contraseña de Oficina Virtual del MH", min_length=13, max_length=25)

    model_config = {"json_schema_extra": {
        "examples": [{"nit": "0614-123456-789-0", "password": "MiClaveSegura123!"}]
    }}


class AuthResponse(BaseModel):
    """Response from MH auth endpoint."""
    status: str
    token: Optional[str] = None
    expires_in: Optional[str] = Field(None, description="Token validity (24h prod, 48h test)")
    environment: str
    message: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# CERTIFICATE (upload per session)
# ─────────────────────────────────────────────────────────────

class CertificateInfo(BaseModel):
    """Information about the loaded certificate."""
    subject: str
    issuer: str
    serial_number: str
    valid_from: str
    valid_to: str
    is_valid: bool
    nit_in_cert: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# DTE TRANSMISSION
# ─────────────────────────────────────────────────────────────

class TransmitRequest(BaseModel):
    """Request to transmit a DTE to MH."""
    dte_json: dict = Field(..., description="DTE completo en formato JSON según esquema MH")
    tipo_dte: TipoDTE = Field(..., description="Tipo de DTE (01, 03, etc.)")
    modelo_transmision: ModeloTransmision = Field(
        default=ModeloTransmision.NORMAL,
        description="1=Normal (síncrono), 2=Contingencia (lote)"
    )

    model_config = {"json_schema_extra": {
        "examples": [{
            "dte_json": {"identificacion": {}, "emisor": {}, "receptor": {}, "cuerpoDocumento": [], "resumen": {}},
            "tipo_dte": "03",
            "modelo_transmision": "1"
        }]
    }}


class TransmitResponse(BaseModel):
    """Response from MH after transmitting a DTE."""
    status: str = Field(..., description="'PROCESADO' si fue aceptado por MH")
    sello_recepcion: Optional[str] = Field(None, description="Sello de Recepción del MH (40 chars)")
    codigo_generacion: Optional[str] = Field(None, description="UUID del DTE")
    numero_control: Optional[str] = Field(None, description="Número de control del DTE")
    fecha_procesamiento: Optional[str] = None
    observaciones: list[str] = Field(default_factory=list)
    clasificacion_msg: Optional[str] = None
    codigo_msg: Optional[str] = None
    descripcion_msg: Optional[str] = None
    raw_response: Optional[dict] = None


# ─────────────────────────────────────────────────────────────
# DTE QUERY
# ─────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    """Request to query a DTE from MH."""
    nit_emisor: str = Field(..., description="NIT del emisor del DTE")
    tipo_dte: TipoDTE = Field(..., description="Tipo de DTE")
    codigo_generacion: str = Field(..., description="UUID (código de generación) del DTE")


class QueryResponse(BaseModel):
    """Response from MH consulta endpoint."""
    status: str
    dte_data: Optional[dict] = None
    sello_recepcion: Optional[str] = None
    fecha_procesamiento: Optional[str] = None
    message: Optional[str] = None
    raw_response: Optional[dict] = None


# ─────────────────────────────────────────────────────────────
# INVALIDATION
# ─────────────────────────────────────────────────────────────

class TipoInvalidacion(str, Enum):
    """CAT-029: Tipo de Invalidación"""
    ERROR_EN_DOCUMENTO = "1"
    NO_CORRESPONDE_OPERACION = "2"
    OTRO = "3"


class TipoResponsable(str, Enum):
    """CAT-032: Responsable de la solicitud"""
    EMISOR = "1"
    RECEPTOR = "2"


class InvalidateRequest(BaseModel):
    """Request to invalidate a previously transmitted DTE."""
    codigo_generacion_doc: str = Field(..., description="UUID del DTE a invalidar")
    tipo_dte: TipoDTE = Field(..., description="Tipo del DTE a invalidar")
    motivo: str = Field(..., description="Motivo de invalidación", min_length=5, max_length=250)
    tipo_invalidacion: TipoInvalidacion = Field(default=TipoInvalidacion.ERROR_EN_DOCUMENTO)
    # Datos del solicitante
    nombre_responsable: str = Field(..., description="Nombre del responsable")
    tipo_documento_responsable: str = Field(default="36", description="CAT-022: 36=NIT, 13=DUI")
    num_documento_responsable: str = Field(..., description="NIT o DUI del responsable")
    tipo_responsable: TipoResponsable = Field(default=TipoResponsable.EMISOR)

    # Datos del emisor
    nit_emisor: str = Field(..., description="NIT del emisor")
    nombre_emisor: str = Field(..., description="Nombre o razón social del emisor")

    # Datos del receptor (obligatorios — MH requiere datos exactos del receptor original)
    nit_receptor: str = Field(..., description="NIT del receptor del DTE original")
    nombre_receptor: str = Field(..., description="Nombre del receptor del DTE original")

    # Datos del DTE original (necesarios para construir el documento de invalidación)
    sello_recibido: str = Field(..., description="Sello de recepción del DTE original (40 chars)")
    numero_control: str = Field(..., description="Número de control del DTE original")
    fecha_emision: str = Field(..., description="Fecha de emisión del DTE original (YYYY-MM-DD)")
    monto_iva: float = Field(default=0.0, description="IVA total del DTE original", ge=0)


class InvalidateResponse(BaseModel):
    """Response from MH invalidation endpoint."""
    status: str
    sello_invalidacion: Optional[str] = None
    fecha_procesamiento: Optional[str] = None
    descripcion_msg: Optional[str] = None
    observaciones: list[str] = Field(default_factory=list)
    raw_response: Optional[dict] = None


# ─────────────────────────────────────────────────────────────
# CONTINGENCY
# ─────────────────────────────────────────────────────────────

class ContingencyRequest(BaseModel):
    """Request for contingency batch transmission."""
    nit_emisor: str
    tipo_contingencia: str = Field(..., description="CAT-006: 1=No disponibilidad MH, 2=No disponibilidad emisor, 5=Otro")
    motivo: str = Field(..., min_length=5, max_length=500)
    fecha_inicio: str = Field(..., description="Fecha/hora inicio contingencia ISO 8601")
    fecha_fin: str = Field(..., description="Fecha/hora fin contingencia ISO 8601")
    detalle_dte: list[dict] = Field(..., description="Lista de DTEs generados en contingencia")


# ─────────────────────────────────────────────────────────────
# GENERIC
# ─────────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str
    detail: str
    code: Optional[str] = None
    mh_observaciones: Optional[list[str]] = None


class HealthResponse(BaseModel):
    """Health check."""
    status: str = "ok"
    version: str
    environment: str
    mh_auth_url: str

