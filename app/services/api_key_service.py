"""
api_key_service.py — API Key management for integrators.

Location: app/services/api_key_service.py
⚠️ NEW FILE — does not modify any existing infrastructure.

Provides:
- generate_api_key(org_id, name, permissions) → key string (shown once)
- validate_api_key(key_string) → {org_id, key_id, permissions} or None
- list_keys(org_id) → list of key metadata
- revoke_key(org_id, key_id)
- rotate_key(org_id, key_id) → new key string
"""

import hashlib
import secrets
import json
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Key format: fsv_live_<32 hex chars> (total 41 chars)
# Prefix stored for display: fsv_live_xxxx****
# Hash stored for lookup: SHA-256 of full key
# ---------------------------------------------------------------------------

KEY_PREFIX = "fsv_live_"


def _generate_raw_key() -> str:
    """Generate a cryptographically secure API key."""
    return KEY_PREFIX + secrets.token_hex(32)


def _hash_key(key: str) -> str:
    """SHA-256 hash of the full key for storage."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _display_prefix(key: str) -> str:
    """First 13 chars visible, rest masked: fsv_live_abcd****"""
    return key[:13] + "****"


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

async def generate_api_key(
    supabase: Any,
    org_id: str,
    created_by: str,
    name: str = "Default",
    permissions: list[str] | None = None,
) -> dict:
    """
    Generate a new API key for an organization.
    Returns the full key (only shown once) + metadata.
    """
    if permissions is None:
        permissions = ["emit", "list", "export", "invalidate"]

    raw_key = _generate_raw_key()
    key_hash = _hash_key(raw_key)
    key_prefix = _display_prefix(raw_key)

    record = {
        "org_id": org_id,
        "key_hash": key_hash,
        "key_prefix": key_prefix,
        "name": name,
        "permissions": json.dumps(permissions),
        "is_active": True,
        "created_by": created_by,
    }

    result = supabase.table("api_keys").insert(record).execute()

    return {
        "key_id": result.data[0]["id"] if result.data else None,
        "api_key": raw_key,  # ⚠️ Only shown ONCE
        "key_prefix": key_prefix,
        "name": name,
        "permissions": permissions,
        "message": "Guarde esta clave de forma segura. No se mostrará de nuevo.",
    }


async def validate_api_key(supabase: Any, key: str) -> dict | None:
    """
    Validate an API key. Returns org context or None if invalid.
    Also updates last_used_at.
    """
    if not key or not key.startswith(KEY_PREFIX):
        return None

    key_hash = _hash_key(key)

    result = supabase.table("api_keys").select(
        "id, org_id, permissions, is_active"
    ).eq("key_hash", key_hash).single().execute()

    if not result.data:
        return None

    data = result.data
    if not data.get("is_active"):
        return None

    # Update last_used_at (best-effort, don't fail on error)
    try:
        supabase.table("api_keys").update({
            "last_used_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", data["id"]).execute()
    except Exception:
        pass

    permissions = data.get("permissions")
    if isinstance(permissions, str):
        permissions = json.loads(permissions)

    return {
        "org_id": data["org_id"],
        "key_id": data["id"],
        "permissions": permissions or [],
    }


async def list_api_keys(supabase: Any, org_id: str) -> list[dict]:
    """List all API keys for an org (never returns full key)."""
    result = supabase.table("api_keys").select(
        "id, key_prefix, name, permissions, is_active, last_used_at, created_at"
    ).eq("org_id", org_id).order("created_at", desc=True).execute()

    keys = []
    for row in (result.data or []):
        perms = row.get("permissions")
        if isinstance(perms, str):
            perms = json.loads(perms)
        keys.append({
            **row,
            "permissions": perms or [],
        })
    return keys


async def revoke_api_key(supabase: Any, org_id: str, key_id: str) -> bool:
    """Revoke (deactivate) an API key."""
    supabase.table("api_keys").update({
        "is_active": False
    }).eq("id", key_id).eq("org_id", org_id).execute()
    return True


async def rotate_api_key(
    supabase: Any, org_id: str, key_id: str
) -> dict | None:
    """Rotate: deactivate old key, generate new one with same name/permissions."""
    # Fetch old key metadata
    old = supabase.table("api_keys").select(
        "name, permissions, created_by"
    ).eq("id", key_id).eq("org_id", org_id).single().execute()

    if not old.data:
        return None

    # Deactivate old
    supabase.table("api_keys").update({
        "is_active": False
    }).eq("id", key_id).execute()

    # Generate new
    perms = old.data.get("permissions")
    if isinstance(perms, str):
        perms = json.loads(perms)

    return await generate_api_key(
        supabase,
        org_id=org_id,
        created_by=old.data.get("created_by", ""),
        name=old.data.get("name", "Rotated"),
        permissions=perms,
    )
