"""
apikeys_router.py — API Key management for POS/ERP integrations
Self-service (dashboard) + Admin management
Keys format: fsv_live_XXXX...  (prefix visible, full key shown once)
"""

import secrets
import hashlib
import logging
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from app.dependencies import get_supabase, get_current_user

logger = logging.getLogger("apikeys")
router = APIRouter(prefix="/api/v1", tags=["api-keys"])


def _generate_key() -> tuple[str, str, str]:
    """Generate API key. Returns (full_key, prefix, hash)."""
    raw = secrets.token_hex(32)
    full_key = f"fsv_live_{raw}"
    prefix = f"fsv_live_{raw[:8]}..."
    key_hash = hashlib.sha256(full_key.encode()).hexdigest()
    return full_key, prefix, key_hash


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


class CreateKeyRequest(BaseModel):
    name: str = Field("Default", description="Nombre descriptivo de la key")
    permissions: list[str] = Field(default=["emit", "query"], description="Permisos: emit, query, webhook")


class AuthenticatedOrg(BaseModel):
    """Result of API key authentication."""
    org_id: str
    key_id: str
    permissions: list[str]


# ── Self-service endpoints (user's own org) ──

@router.post("/api-keys")
async def create_api_key(
    body: CreateKeyRequest,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Create a new API key for user's org. Full key shown ONCE."""
    org_id = user["org_id"]

    # Max 5 keys per org
    existing = supabase.table("api_keys").select("id", count="exact").eq("org_id", org_id).execute()
    if (existing.count or 0) >= 5:
        raise HTTPException(400, "Maximo 5 API keys por organizacion")

    full_key, prefix, key_hash = _generate_key()

    supabase.table("api_keys").insert({
        "org_id": org_id,
        "key_prefix": prefix,
        "key_hash": key_hash,
        "name": body.name,
        "permissions": body.permissions,
        "is_active": True,
        "created_by": "dashboard",
    }).execute()

    return {
        "api_key": full_key,
        "prefix": prefix,
        "name": body.name,
        "permissions": body.permissions,
        "warning": "Guarde esta clave. No se mostrara de nuevo.",
    }


@router.get("/api-keys")
async def list_api_keys(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """List API keys for user's org (prefix only, never full key)."""
    org_id = user["org_id"]
    result = supabase.table("api_keys").select(
        "id,key_prefix,name,permissions,is_active,last_used_at,created_by,created_at"
    ).eq("org_id", org_id).order("created_at", desc=True).execute()
    return {"api_keys": result.data or []}


@router.patch("/api-keys/{key_id}/toggle")
async def toggle_api_key(
    key_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Activate/deactivate an API key."""
    org_id = user["org_id"]
    existing = supabase.table("api_keys").select("id,is_active").eq("id", key_id).eq("org_id", org_id).single().execute()
    if not existing.data:
        raise HTTPException(404, "API key no encontrada")

    new_status = not existing.data["is_active"]
    supabase.table("api_keys").update({"is_active": new_status}).eq("id", key_id).execute()
    return {"id": key_id, "is_active": new_status}


@router.delete("/api-keys/{key_id}")
async def delete_api_key(
    key_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Permanently delete an API key."""
    supabase.table("api_keys").delete().eq("id", key_id).eq("org_id", user["org_id"]).execute()
    return {"deleted": True, "id": key_id}


# ── API Key authentication middleware ──

async def authenticate_api_key(request: Request, supabase=Depends(get_supabase)) -> AuthenticatedOrg:
    """
    Authenticate request via API key in header: Authorization: Bearer fsv_live_...
    Use as Depends() in DTE emission endpoints for external integrations.
    """
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer fsv_live_"):
        raise HTTPException(401, "API key invalida")

    key = auth.replace("Bearer ", "")
    key_hash = _hash_key(key)

    result = supabase.table("api_keys").select(
        "id,org_id,permissions,is_active"
    ).eq("key_hash", key_hash).single().execute()

    if not result.data:
        raise HTTPException(401, "API key no encontrada")
    if not result.data["is_active"]:
        raise HTTPException(403, "API key desactivada")

    # Update last_used_at
    supabase.table("api_keys").update(
        {"last_used_at": datetime.now(timezone.utc).isoformat()}
    ).eq("id", result.data["id"]).execute()

    return AuthenticatedOrg(
        org_id=result.data["org_id"],
        key_id=result.data["id"],
        permissions=result.data["permissions"],
    )
