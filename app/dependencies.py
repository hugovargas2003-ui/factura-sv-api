"""
FACTURA-SV: Dependencias FastAPI
=================================
Inyección de dependencias para autenticación y servicios.
"""
import os
from functools import lru_cache

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase import create_client, Client as SupabaseClient

from app.services.encryption_service import EncryptionService
from app.services.dte_service import DTEService

# ── Security scheme ──
security = HTTPBearer()


# ── Singletons ──

@lru_cache()
def get_supabase() -> SupabaseClient:
    """Supabase client singleton (service role)."""
    return create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
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
    supabase: SupabaseClient = Depends(get_supabase),
) -> dict:
    """
    Valida JWT de Supabase y retorna {user_id, org_id, email, role}.
    Usado como Depends() en todos los endpoints protegidos.
    """
    token = credentials.credentials

    try:
        # Verificar token con Supabase
        user_response = supabase.auth.get_user(token)
        user = user_response.user

        if not user:
            raise HTTPException(401, "Token inválido o expirado")

    except Exception:
        raise HTTPException(401, "Token inválido o expirado")

    # Obtener org_id y role del usuario
    result = supabase.table("users").select(
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
