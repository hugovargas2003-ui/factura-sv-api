"""
FACTURA-SV — Main API Application
FastAPI backend for DTE electronic invoicing in El Salvador.

Complete flow:
  1. POST /auth          → Authenticate with MH (get JWT token)
  2. POST /certificate   → Upload .p12 certificate (per session, ephemeral)
  3. POST /transmit      → Sign DTE + transmit to MH (returns sello de recepción)
  4. POST /query         → Query DTE status from MH
  5. POST /invalidate    → Invalidate a previously accepted DTE

Architecture:
  - User provides their OWN credentials (NIT + password + .p12)
  - FACTURA-SV never stores credentials or certificates to disk
  - Certificates exist only in memory for the duration of the session
  - Tokens are cached in-memory and auto-renewed
"""

import logging
import uuid
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings, get_mh_url
from app.schemas.models import (
    AuthRequest, AuthResponse,
    CertificateInfo,
    TransmitRequest, TransmitResponse,
    QueryRequest, QueryResponse,
    InvalidateRequest, InvalidateResponse,
    ErrorResponse, HealthResponse,
)
from app.modules.auth_bridge import auth_bridge, AuthBridgeError, TokenInfo
from app.modules.sign_engine import sign_engine, SignEngineError, CertificateSession
from app.modules.transmit_service import transmit_service, TransmitError
from app.modules.query_service import query_service, QueryError
from app.modules.invalidation_service import invalidation_service, InvalidationError
from app.utils.dte_helpers import generate_codigo_generacion, validate_nit

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("factura-sv")


# ─────────────────────────────────────────────────────────────
# IN-MEMORY SESSION STORE
# Stores active certificate sessions keyed by session_id.
# In production, use Redis or similar with TTL.
# ─────────────────────────────────────────────────────────────

_sessions: dict[str, dict] = {}
# Structure: { session_id: { "cert": CertificateSession, "token": TokenInfo, "nit": str, "created": datetime, "last_accessed": datetime } }

SESSION_MAX_INACTIVE_HOURS = 24
SESSION_CLEANUP_INTERVAL_SECONDS = 3600  # 1 hour


def _create_session(token_info: TokenInfo, cert_session: CertificateSession = None) -> str:
    """Create a new session and return its ID."""
    session_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    _sessions[session_id] = {
        "token": token_info,
        "cert": cert_session,
        "nit": token_info.nit,
        "created": now,
        "last_accessed": now,
    }
    return session_id


def _get_session(session_id: str) -> dict:
    """Get session data or raise 401."""
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Sesión no encontrada. Autentíquese primero con POST /auth")
    if session["token"].is_expired:
        # Destroy cert before removing session
        if session.get("cert"):
            session["cert"].destroy()
        del _sessions[session_id]
        raise HTTPException(status_code=401, detail="Token MH expirado. Re-autentíquese con POST /auth")
    session["last_accessed"] = datetime.now(timezone.utc)
    return session


async def _cleanup_stale_sessions():
    """Background task: periodically remove inactive sessions."""
    while True:
        await asyncio.sleep(SESSION_CLEANUP_INTERVAL_SECONDS)
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=SESSION_MAX_INACTIVE_HOURS)
        stale = [
            sid for sid, s in _sessions.items()
            if s["last_accessed"] < cutoff or s["token"].is_expired
        ]
        for sid in stale:
            session = _sessions.pop(sid, None)
            if session and session.get("cert"):
                session["cert"].destroy()
        if stale:
            logger.info(f"Session cleanup: removed {len(stale)} stale session(s). Active: {len(_sessions)}")


# ─────────────────────────────────────────────────────────────
# APP LIFECYCLE
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 FACTURA-SV v{settings.app_version} starting...")
    logger.info(f"   Environment: {settings.mh_environment.value}")
    logger.info(f"   MH Auth URL: {get_mh_url('auth')}")
    logger.info(f"   MH Recepción URL: {get_mh_url('recepcion_dte')}")
    # Start background session cleanup
    cleanup_task = asyncio.create_task(_cleanup_stale_sessions())
    yield
    # Shutdown: cancel cleanup and destroy all sessions
    cleanup_task.cancel()
    for sid, session in _sessions.items():
        if session.get("cert"):
            session["cert"].destroy()
    _sessions.clear()
    logger.info("FACTURA-SV shutdown complete. All sessions destroyed.")


# ─────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────


# --- Rate Limiting ---
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

def _get_rate_limit_key(request):
    """Rate limit by org_id if authenticated, else by IP."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        try:
            import jwt
            token = auth_header.split(" ", 1)[1]
            # Decode without verification just to get sub — actual auth happens in dependency
            payload = jwt.decode(token, options={"verify_signature": False})
            return payload.get("sub", get_remote_address(request))
        except Exception:
            pass
    return get_remote_address(request)

limiter = Limiter(key_func=_get_rate_limit_key, default_limits=["60/minute"])

app = FastAPI(
    title="FACTURA-SV API",
    description=(
        "Backend API para facturación electrónica DTE en El Salvador. "
        "Conecta con la API del Ministerio de Hacienda para transmitir, "
        "consultar e invalidar Documentos Tributarios Electrónicos."
    ),
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

_cors_origins = ["*"] if settings.debug else [
    "https://factura-sv.algoritmos.io",
    "https://algoritmos.io",
    "https://factura-sv-production-70de.up.railway.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiter setup
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


# ─────────────────────────────────────────────────────────────
# GLOBAL EXCEPTION HANDLERS
# ─────────────────────────────────────────────────────────────

@app.exception_handler(AuthBridgeError)
async def auth_error_handler(request: Request, exc: AuthBridgeError):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error="AUTH_ERROR", detail=exc.message, code="AUTH_FAILED",
            mh_observaciones=None,
        ).model_dump(),
    )


@app.exception_handler(SignEngineError)
async def sign_error_handler(request: Request, exc: SignEngineError):
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            error="SIGN_ERROR", detail=exc.message, code=exc.code,
        ).model_dump(),
    )


@app.exception_handler(TransmitError)
async def transmit_error_handler(request: Request, exc: TransmitError):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error="TRANSMIT_ERROR", detail=exc.message, code="MH_REJECTED",
            mh_observaciones=exc.observaciones,
        ).model_dump(),
    )


@app.exception_handler(QueryError)
async def query_error_handler(request: Request, exc: QueryError):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error="QUERY_ERROR", detail=exc.message, code="QUERY_FAILED",
        ).model_dump(),
    )


@app.exception_handler(InvalidationError)
async def invalidation_error_handler(request: Request, exc: InvalidationError):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error="INVALIDATION_ERROR", detail=exc.message,
            code="INVALIDATION_FAILED",
            mh_observaciones=exc.observaciones,
        ).model_dump(),
    )


# ═════════════════════════════════════════════════════════════
# ROUTES
# ═════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Sistema"])
async def health_check():
    """Verificar estado del servicio."""
    return {
        "status": "ok",
        "version": settings.app_version,
        "environment": settings.mh_environment.value,
        "mh_auth_url": get_mh_url("auth"),
    }


# ─────────────────────────────────────────────────────────────
# STEP 1: AUTHENTICATE WITH MH
# ─────────────────────────────────────────────────────────────

@app.post(
    "/auth",
    response_model=AuthResponse,
    tags=["1. Autenticación"],
    summary="Autenticar con el Ministerio de Hacienda",
    description=(
        "Envía las credenciales de Oficina Virtual del contribuyente al MH "
        "y obtiene un token JWT. El token es válido por 24h (producción) o "
        "48h (test). Retorna un `session_id` en el header `X-Session-Id` "
        "que debe enviarse en todas las solicitudes posteriores."
    ),
)
async def authenticate(request: AuthRequest):
    # Validate NIT format
    if not validate_nit(request.nit):
        raise HTTPException(
            status_code=422,
            detail=f"Formato de NIT inválido: '{request.nit}'. Formato esperado: 0614-XXXXXX-XXX-X",
        )

    # Authenticate with MH
    token_info = await auth_bridge.authenticate(
        nit=request.nit,
        password=request.password,
    )

    # Create session
    session_id = _create_session(token_info)

    validity = "24 horas" if settings.mh_environment.value == "production" else "48 horas"

    response = AuthResponse(
        status="authenticated",
        token=None,  # Don't expose MH token to user
        expires_in=validity,
        environment=settings.mh_environment.value,
        message=f"Autenticación exitosa. Use el header X-Session-Id: {session_id} en las siguientes solicitudes.",
    )

    return JSONResponse(
        content=response.model_dump(),
        headers={"X-Session-Id": session_id},
    )


# ─────────────────────────────────────────────────────────────
# STEP 2: UPLOAD CERTIFICATE (.p12)
# ─────────────────────────────────────────────────────────────

@app.post(
    "/certificate",
    response_model=CertificateInfo,
    tags=["2. Certificado"],
    summary="Cargar certificado de firma electrónica (.p12)",
    description=(
        "Suba el archivo .p12/.pfx emitido por la DGII junto con su contraseña. "
        "El certificado se mantiene SOLO en memoria durante la sesión. "
        "No se persiste en disco ni en base de datos. "
        "Al cerrar la sesión o al expirar el token, el certificado se destruye."
    ),
)
async def upload_certificate(
    certificate: UploadFile = File(..., description="Archivo .p12 o .pfx"),
    password: str = Form(..., description="Contraseña del certificado"),
    session_id: str = Form(..., description="Session ID obtenido en /auth"),
):
    session = _get_session(session_id)

    # Validate file type
    filename = certificate.filename or ""
    if not filename.lower().endswith((".p12", ".pfx")):
        raise HTTPException(
            status_code=422,
            detail="El archivo debe ser .p12 o .pfx",
        )

    # Read file into memory
    p12_data = await certificate.read()

    if len(p12_data) == 0:
        raise HTTPException(status_code=422, detail="El archivo está vacío.")

    if len(p12_data) > 50_000:  # .p12 files are typically < 10KB
        raise HTTPException(status_code=422, detail="El archivo es demasiado grande (máximo 50KB).")

    # Validate PKCS#12 magic number (ASN.1 SEQUENCE tag)
    # PKCS#12/PFX files start with 0x30 0x82 (DER-encoded ASN.1 SEQUENCE)
    if len(p12_data) < 4 or p12_data[0] != 0x30:
        raise HTTPException(
            status_code=422,
            detail="El archivo no parece ser un PKCS#12 (.p12/.pfx) válido. Verifique que subió el archivo correcto.",
        )

    # Load certificate (SignEngine validates expiry, password, etc.)
    cert_session = sign_engine.load_certificate(p12_data, password)

    # Destroy previous cert if exists
    if session.get("cert"):
        session["cert"].destroy()

    # Attach to session
    session["cert"] = cert_session

    # SECURITY: Verify NIT in certificate matches authenticated NIT
    nit_in_cert = cert_session.get_nit_from_subject()
    session_nit = session["nit"]
    if nit_in_cert and nit_in_cert != session_nit:
        cert_session.destroy()
        raise HTTPException(
            status_code=403,
            detail=(
                f"El NIT del certificado ({nit_in_cert}) no coincide con el NIT "
                f"autenticado ({session_nit}). No puede usar un certificado de otra empresa."
            ),
        )

    logger.info(f"Certificate loaded for session {session_id[:8]}..., NIT in cert: {nit_in_cert}")

    return cert_session.to_dict()


# ─────────────────────────────────────────────────────────────
# STEP 3: SIGN + TRANSMIT DTE
# ─────────────────────────────────────────────────────────────

@app.api_route(
    "/transmit",
    methods=["POST"],
    response_model=TransmitResponse,
    tags=["3. Transmisión"],
)
async def transmit_dte_handler(request: Request):
    """
    Firmar y transmitir DTE al Ministerio de Hacienda.

    Firma el DTE JSON con el certificado .p12 de la sesión,
    luego lo transmite al MH. Retorna sello de recepción si
    es aceptado, u observaciones si es rechazado.

    Requiere: POST /auth + POST /certificate previos.
    Header requerido: X-Session-Id
    Body: { "dte_json": {...}, "tipo_dte": "03" }
    """
    # Get session ID from header
    session_id = request.headers.get("X-Session-Id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Header X-Session-Id requerido.")

    session = _get_session(session_id)

    # Verify certificate is loaded
    cert_session: CertificateSession = session.get("cert")
    if not cert_session:
        raise HTTPException(
            status_code=422,
            detail="No hay certificado cargado. Primero suba el .p12 con POST /certificate",
        )

    # Parse JSON body
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Body debe ser JSON válido.")

    dte_json = body.get("dte_json")
    tipo_dte = body.get("tipo_dte")

    if not dte_json or not tipo_dte:
        raise HTTPException(
            status_code=422,
            detail="Campos requeridos: dte_json (dict), tipo_dte (string)",
        )

    # Extract codigoGeneracion from DTE or generate one
    identificacion = dte_json.get("identificacion", {})
    codigo_generacion = identificacion.get("codigoGeneracion")
    if not codigo_generacion:
        codigo_generacion = generate_codigo_generacion()
        if "identificacion" not in dte_json:
            dte_json["identificacion"] = {}
        dte_json["identificacion"]["codigoGeneracion"] = codigo_generacion
        logger.info(f"Auto-generated codigoGeneracion: {codigo_generacion}")

    # STEP A: Sign the DTE
    logger.info(f"Signing DTE type={tipo_dte}, codGen={codigo_generacion[:8]}...")
    signed_jwt = sign_engine.sign_dte(cert_session, dte_json)

    # STEP B: Transmit to MH
    logger.info(f"Transmitting to MH...")
    token_info: TokenInfo = session["token"]
    result = await transmit_service.transmit(
        token_info=token_info,
        signed_dte=signed_jwt,
        tipo_dte=tipo_dte,
        codigo_generacion=codigo_generacion,
    )


    # --- Notificación por email al receptor (no-bloqueante) ---
    if result.status == "PROCESADO":
        try:
            from app.routers.email_router import notify_dte_by_email
            from app.services.pdf_generator import DTEPdfGenerator
            pdf_bytes = DTEPdfGenerator(dte_json, sello=result.sello_recepcion).generate()
            email_result = await notify_dte_by_email(
                dte_json=dte_json,
                pdf_bytes=pdf_bytes,
                sello_recibido=result.sello_recepcion,
            )
            logger.info(f"Email DTE con PDF: {email_result.get('message', 'N/A')}")

        except Exception as e:
            logger.warning(f"Email DTE falló (no-bloqueante): {e}")

    return result


# ─────────────────────────────────────────────────────────────
# STEP 4: QUERY DTE
# ─────────────────────────────────────────────────────────────

@app.api_route("/query", methods=["POST"], response_model=QueryResponse, tags=["4. Consulta"])
async def query_dte_handler(request: Request):
    """
    Consultar estado de un DTE en el Ministerio de Hacienda.

    Busca un DTE previamente transmitido usando el NIT emisor,
    tipo de DTE y código de generación (UUID).
    """
    session_id = request.headers.get("X-Session-Id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Header X-Session-Id requerido.")

    session = _get_session(session_id)

    body = await request.json()
    nit_emisor = body.get("nit_emisor")
    tipo_dte = body.get("tipo_dte")
    codigo_generacion = body.get("codigo_generacion")

    if not all([nit_emisor, tipo_dte, codigo_generacion]):
        raise HTTPException(
            status_code=422,
            detail="Campos requeridos: nit_emisor, tipo_dte, codigo_generacion",
        )

    token_info: TokenInfo = session["token"]
    result = await query_service.query(
        token_info=token_info,
        nit_emisor=nit_emisor,
        tipo_dte=tipo_dte,
        codigo_generacion=codigo_generacion,
    )

    return result


# ─────────────────────────────────────────────────────────────
# STEP 5: INVALIDATE DTE
# ─────────────────────────────────────────────────────────────

@app.api_route("/invalidate", methods=["POST"], response_model=InvalidateResponse, tags=["5. Invalidación"])
async def invalidate_dte_handler(request: Request):
    """
    Invalidar (anular) un DTE previamente aceptado por el MH.

    Requiere certificado cargado (el documento de invalidación
    también debe firmarse). El DTE debe estar dentro del plazo
    de invalidación (90 días para tipos 01, 03, 11, 14).

    **Campos requeridos en el body:**
    - codigo_generacion_doc: UUID del DTE original
    - tipo_dte: tipo del DTE original
    - motivo: razón de invalidación
    - sello_recibido: sello del DTE original
    - numero_control: número de control del DTE original
    - fecha_emision: fecha de emisión del DTE original
    - nit_emisor, nombre_emisor: datos del emisor
    - nit_receptor, nombre_receptor: datos del receptor del DTE original
    - nombre_responsable, num_documento_responsable: solicitante
    """
    session_id = request.headers.get("X-Session-Id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Header X-Session-Id requerido.")

    session = _get_session(session_id)

    cert_session: CertificateSession = session.get("cert")
    if not cert_session:
        raise HTTPException(
            status_code=422,
            detail="Certificado requerido para invalidar. Cargue .p12 con POST /certificate",
        )

    body = await request.json()

    # Validate all fields via Pydantic model (including sello_recibido,
    # numero_control, fecha_emision, monto_iva, nit_receptor, nombre_receptor)
    try:
        inv_request = InvalidateRequest(**body)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Campos faltantes o inválidos: {str(e)}")

    # Build the invalidation document (all data comes from the validated model)
    invalidation_doc = invalidation_service.build_invalidation_document(
        request=inv_request,
    )

    # Sign + transmit invalidation
    token_info: TokenInfo = session["token"]
    result = await invalidation_service.invalidate(
        token_info=token_info,
        cert_session=cert_session,
        invalidation_doc=invalidation_doc,
    )

    return result


# ─────────────────────────────────────────────────────────────
# SESSION MANAGEMENT
# ─────────────────────────────────────────────────────────────

@app.get("/session", tags=["Sesión"], summary="Ver información de la sesión activa")
async def get_session_info(request: Request):
    """Retorna información de la sesión sin exponer datos sensibles."""
    session_id = request.headers.get("X-Session-Id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Header X-Session-Id requerido.")

    session = _get_session(session_id)

    return {
        "session_id": session_id,
        "nit": session["nit"],
        "created": session["created"].isoformat(),
        "token_info": session["token"].to_dict(),
        "certificate_loaded": session.get("cert") is not None,
        "certificate_info": session["cert"].to_dict() if session.get("cert") else None,
    }


@app.delete("/session", tags=["Sesión"], summary="Cerrar sesión y destruir certificado")
async def destroy_session(request: Request):
    """
    Cierra la sesión: destruye el certificado de memoria y revoca el token cacheado.
    """
    session_id = request.headers.get("X-Session-Id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Header X-Session-Id requerido.")

    session = _sessions.get(session_id)
    if session:
        if session.get("cert"):
            session["cert"].destroy()
        del _sessions[session_id]
        logger.info(f"Session {session_id[:8]}... destroyed.")

    return {"status": "session_destroyed", "message": "Certificado eliminado de memoria. Sesión cerrada."}


# ─────────────────────────────────────────────────────────────
# UTILITY: GENERATE DTE IDENTIFIERS
# ─────────────────────────────────────────────────────────────

@app.get("/utils/generate-uuid", tags=["Utilidades"], summary="Generar UUID v4 para codigoGeneracion")
async def generate_uuid():
    """Genera un UUID v4 válido para usar como codigoGeneracion de un DTE."""
    return {"codigoGeneracion": generate_codigo_generacion()}


@app.get(
    "/utils/generate-numero-control",
    tags=["Utilidades"],
    summary="Generar número de control",
)
async def generate_control_number(
    tipo_dte: str = "03",
    establecimiento: str = "M001",
    punto_venta: str = "P001",
    correlativo: int = 1,
):
    """
    Genera un número de control DTE con el formato:
    DTE-TT-SSSS-PPPP-NNNNNNNNNNNNNNN
    """
    from app.utils.dte_helpers import generate_numero_control
    return {
        "numeroControl": generate_numero_control(
            tipo_dte, establecimiento, punto_venta, correlativo
        )
    }


# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# SAAS LAYER: DTE EMISSION (Sprint 1)
# ─────────────────────────────────────────────────────────────

from app.dependencies import get_dte_service, get_current_user, get_current_user_or_api_key
from app.routers.dte_router import create_dte_router

# --- Public DTE Verification (no auth required) ---
@app.get("/api/v1/verificar/{codigo_generacion}", tags=["Verificación Pública"])
@limiter.limit("30/minute")
async def verificar_dte(codigo_generacion: str, request: Request):
    """Public endpoint — verify DTE authenticity without login."""
    from app.dependencies import get_supabase
    db = get_supabase()
    result = db.table("dtes").select(
        "tipo_dte, numero_control, codigo_generacion, fecha_emision, "
        "hora_emision, receptor_nombre, receptor_nit, monto_total, "
        "total_gravada, total_exenta, iva, estado, sello_recibido, created_at"
    ).eq("codigo_generacion", codigo_generacion).execute()

    if not result.data:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={
            "encontrado": False,
            "mensaje": "No se encontró ningún DTE con este código de generación."
        })

    dte = result.data[0]
    # Get emisor name from dte_credentials via org relationship
    org_result = db.table("dtes").select("org_id").eq(
        "codigo_generacion", codigo_generacion
    ).execute()
    emisor_nombre = None
    if org_result.data:
        org_id = org_result.data[0]["org_id"]
        cred_result = db.table("dte_credentials").select("nombre, nit").eq(
            "org_id", org_id
        ).execute()
        if cred_result.data:
            emisor_nombre = cred_result.data[0].get("nombre")

    tipo_nombres = {
        "01": "Factura", "03": "Comprobante de Crédito Fiscal",
        "04": "Nota de Remisión", "05": "Nota de Crédito",
        "06": "Nota de Débito", "07": "Comprobante de Retención",
        "08": "Comprobante de Liquidación", "09": "Documento Contable de Liquidación",
        "11": "Factura de Sujeto Excluido", "14": "Factura de Exportación",
        "15": "Comprobante de Donación"
    }

    return {
        "encontrado": True,
        "dte": {
            "tipo_dte": dte["tipo_dte"],
            "tipo_nombre": tipo_nombres.get(dte["tipo_dte"], dte["tipo_dte"]),
            "numero_control": dte["numero_control"],
            "codigo_generacion": dte["codigo_generacion"],
            "fecha_emision": dte["fecha_emision"],
            "hora_emision": dte["hora_emision"],
            "emisor_nombre": emisor_nombre,
            "receptor_nombre": dte["receptor_nombre"],
            "monto_total": dte["monto_total"],
            "estado": dte["estado"],
            "sello_mh": dte["sello_recibido"],
            "verificado_mh": dte["sello_recibido"] is not None and dte["estado"] == "procesado"
        }
    }


_dte_router = create_dte_router(
    get_dte_service=get_dte_service,
    get_current_user=get_current_user_or_api_key,
)
app.include_router(_dte_router)

from app.routers.billing_router import router as billing_router
app.include_router(billing_router)

from app.routers.admin_router import router as admin_router
app.include_router(admin_router, prefix="/api/v1")

# --- Email DTE Service ---
from app.routers.email_router import router as email_router
app.include_router(email_router, prefix="/api/v1")



# ENTRYPOINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


from app.routers.config_router import router as config_router
app.include_router(config_router, prefix="/api/v1")

from app.routers.cron_router import router as cron_router
app.include_router(cron_router, prefix="/api/v1")

from app.routers.credits_router import router as credits_router
app.include_router(credits_router)

from app.routers.receptor_router import router as receptor_router
app.include_router(receptor_router)

from app.routers.dte_recibidos_router import router as dte_recibidos_router
app.include_router(dte_recibidos_router)

from app.routers.reconciliacion_router import router as reconciliacion_router
app.include_router(reconciliacion_router)

from app.routers.apikeys_router import router as apikeys_router
app.include_router(apikeys_router)

from app.routers.org_whatsapp_router import router as org_whatsapp_router
app.include_router(org_whatsapp_router)

from app.routers.legal_router import router as legal_router
from app.routers.catalog_router import router as catalog_router
app.include_router(legal_router)
app.include_router(catalog_router, prefix="/api/v1")
