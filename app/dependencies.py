"""
FACTURA-SV: Dependencias FastAPI
=================================
Inyección de dependencias para autenticación y servicios.

ARQUITECTURA DE CLIENTS SUPABASE:
- _auth_client: SOLO para validar JWT (auth.get_user). Nunca para queries.
- get_supabase(): Client limpio para queries de datos. Nunca toca auth.
Esto evita que auth.get_user() contamine el contexto del client de datos.
"""
import os
import logging
from functools import lru_cache

from typing import Optional
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase import create_client, Client as SupabaseClient

from app.services.encryption_service import EncryptionService
from app.services.dte_service import DTEService

logger = logging.getLogger(__name__)

# ── Security scheme ──
security = HTTPBearer()


def _clean_env(key: str) -> str:
    """Sanitiza variable de entorno: elimina whitespace y comillas residuales."""
    return os.environ[key].strip().strip('"').strip("'")


# ── Supabase Clients ──

@lru_cache()
def _get_auth_client() -> SupabaseClient:
    """Client dedicado SOLO para auth.get_user(). Nunca usar para queries."""
    return create_client(
        _clean_env("SUPABASE_URL"),
        _clean_env("SUPABASE_SERVICE_ROLE_KEY"),
    )


@lru_cache()
def get_supabase() -> SupabaseClient:
    """Client limpio para queries de datos. Nunca toca auth.get_user()."""
    return create_client(
        _clean_env("SUPABASE_URL"),
        _clean_env("SUPABASE_SERVICE_ROLE_KEY"),
    )


@lru_cache()
def get_encryption() -> EncryptionService:
    """Encryption service singleton."""
    return EncryptionService()


def get_dte_service(
    supabase: SupabaseClient = Depends(get_supabase),
    encryption: EncryptionService = Depends(get_encryption),
) -> DTEService:
    """DTE service con Supabase y encriptación inyectados."""
    return DTEService(supabase=supabase, encryption=encryption)


# ── Auth dependency ──

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    """
    Valida JWT de Supabase y retorna {user_id, org_id, email, role}.
    Usa _auth_client dedicado para no contaminar el client de datos.
    """
    token = credentials.credentials
    auth_client = _get_auth_client()
    data_client = get_supabase()

    try:
        user_response = auth_client.auth.get_user(token)
        user = user_response.user
        if not user:
            raise HTTPException(401, "Token inválido o expirado")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Token inválido o expirado")

    # Query con el client LIMPIO (no contaminado por auth)
    result = data_client.table("users").select(
        "org_id, role, email, full_name"
    ).eq("id", user.id).single().execute()

    if not result.data:
        raise HTTPException(403, "Usuario sin organización asignada")

    return {
        "user_id": user.id,
        "org_id": result.data["org_id"],
        "email": result.data.get("email", ""),
        "role": result.data.get("role", "member"),
        "full_name": result.data.get("full_name", ""),
    }


# ── Dual Auth: JWT or API Key ──

async def get_current_user_or_api_key(
    request: Request,
) -> dict:
    """
    Authenticate via JWT (web dashboard) or API key (integrators).
    Returns user dict with extra field 'auth_source': 'web' | 'api'.
    """
    from app.services.api_key_service import validate_api_key

    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(401, "Authorization header requerido")

    token = auth_header.replace("Bearer ", "").strip()
    data_client = get_supabase()

    # Check if it's an API key (starts with fsv_live_)
    if token.startswith("fsv_live_"):
        result = await validate_api_key(data_client, token)
        if not result:
            raise HTTPException(401, "API key invalida o inactiva")
        return {
            "user_id": result.get("created_by", "api"),
            "org_id": result["org_id"],
            "email": "",
            "role": "api_integrator",
            "full_name": f"API: {result.get('name', 'key')}",
            "auth_source": "api",
            "api_key_id": result.get("key_id"),
        }

    # Otherwise, validate as JWT
    auth_client = _get_auth_client()
    try:
        user_response = auth_client.auth.get_user(token)
        user = user_response.user
        if not user:
            raise HTTPException(401, "Token invalido o expirado")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Token invalido o expirado")

    result = data_client.table("users").select(
        "org_id, role, email, full_name"
    ).eq("id", user.id).single().execute()

    if not result.data:
        raise HTTPException(403, "Usuario sin organizacion asignada")

    return {
        "user_id": user.id,
        "org_id": result.data["org_id"],
        "email": result.data.get("email", ""),
        "role": result.data.get("role", "member"),
        "full_name": result.data.get("full_name", ""),
        "auth_source": "web",
    }
