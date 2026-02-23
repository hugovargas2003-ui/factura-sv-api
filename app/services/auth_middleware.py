"""
auth_middleware.py — Extended auth with API key support and role permissions.

Location: app/services/auth_middleware.py
⚠️ NEW FILE — does not modify dependencies.py or any existing auth.

Provides:
- get_current_user_or_api_key: accepts JWT OR X-API-Key header
- require_role(*roles): dependency that checks user role
- require_permission(*perms): dependency for API key permissions
"""

from fastapi import Depends, HTTPException, Request
from typing import Callable

from app.dependencies import get_current_user, get_supabase
from app.services.api_key_service import validate_api_key


# ---------------------------------------------------------------------------
# S5-1: Dual auth — JWT or API Key
# ---------------------------------------------------------------------------

async def get_current_user_or_api_key(request: Request) -> dict:
    """
    Authenticate via Supabase JWT (Authorization: Bearer xxx)
    OR via API Key (X-API-Key: fsv_live_xxx).

    Returns dict with:
    - user_id, org_id, email, role, full_name (for JWT)
    - org_id, key_id, permissions, auth_type="api_key" (for API key)
    """
    # Check for API key first
    api_key = request.headers.get("X-API-Key")
    if api_key:
        supabase = get_supabase()
        key_data = await validate_api_key(supabase, api_key)
        if not key_data:
            raise HTTPException(401, "API key inválida o revocada")
        return {
            "org_id": key_data["org_id"],
            "key_id": key_data["key_id"],
            "permissions": key_data["permissions"],
            "auth_type": "api_key",
            "user_id": None,
            "email": None,
            "role": "api_key",
            "full_name": "API Key",
        }

    # Fallback to JWT
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(401, "Se requiere Authorization header o X-API-Key")

    # Use existing get_current_user logic
    from fastapi.security import HTTPAuthorizationCredentials
    creds = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=auth_header.replace("Bearer ", ""),
    )
    supabase = get_supabase()
    user = await get_current_user.__wrapped__(creds, supabase) if hasattr(get_current_user, '__wrapped__') else None

    # If __wrapped__ doesn't work, do it inline
    if user is None:
        token = auth_header.replace("Bearer ", "")
        try:
            user_response = supabase.auth.get_user(token)
            u = user_response.user
            if not u:
                raise HTTPException(401, "Token inválido")
        except Exception:
            raise HTTPException(401, "Token inválido o expirado")

        result = supabase.table("users").select(
            "org_id, role, email, full_name"
        ).eq("id", u.id).single().execute()

        if not result.data:
            raise HTTPException(403, "Usuario sin organización")

        user = {
            "user_id": u.id,
            "org_id": result.data["org_id"],
            "email": result.data.get("email", ""),
            "role": result.data.get("role", "member"),
            "full_name": result.data.get("full_name", ""),
            "auth_type": "jwt",
        }

    user["auth_type"] = user.get("auth_type", "jwt")
    return user


# ---------------------------------------------------------------------------
# S5-2: Role-based access control
# ---------------------------------------------------------------------------

# Role hierarchy: admin > emisor > auditor
ROLE_HIERARCHY = {
    "admin": 3,
    "emisor": 2,
    "auditor": 1,
    "member": 2,  # member = emisor level
}

# What each role can do
ROLE_PERMISSIONS = {
    "admin": {"emit", "list", "export", "invalidate", "config", "users", "billing", "keys"},
    "emisor": {"emit", "list", "export"},
    "member": {"emit", "list", "export"},
    "auditor": {"list", "export"},
    "api_key": set(),  # determined by key permissions
}


def require_role(*allowed_roles: str) -> Callable:
    """
    Dependency factory: require user has one of the allowed roles.

    Usage:
        @router.post("/endpoint")
        async def handler(user=Depends(require_role("admin", "emisor"))):
            ...
    """
    async def _check_role(
        user: dict = Depends(get_current_user),
    ) -> dict:
        role = user.get("role", "member")

        # API keys: check if they have equivalent permission
        if user.get("auth_type") == "api_key":
            # API keys are allowed if endpoint doesn't require specific role
            if "api_key" in allowed_roles:
                return user
            raise HTTPException(403, "API keys no tienen acceso a esta función")

        if role not in allowed_roles:
            raise HTTPException(
                403,
                f"Rol '{role}' no tiene permiso. Se requiere: {', '.join(allowed_roles)}"
            )
        return user

    return _check_role


def require_permission(*required_perms: str) -> Callable:
    """
    Dependency factory: require user/key has specific permissions.
    Works for both JWT users (via role mapping) and API keys.

    Usage:
        @router.post("/endpoint")
        async def handler(user=Depends(require_permission("emit"))):
            ...
    """
    async def _check_permission(
        user: dict = Depends(get_current_user),
    ) -> dict:
        if user.get("auth_type") == "api_key":
            key_perms = set(user.get("permissions", []))
            missing = set(required_perms) - key_perms
            if missing:
                raise HTTPException(
                    403,
                    f"API key no tiene permisos: {', '.join(missing)}"
                )
            return user

        # JWT user: derive permissions from role
        role = user.get("role", "member")
        role_perms = ROLE_PERMISSIONS.get(role, set())
        missing = set(required_perms) - role_perms
        if missing:
            raise HTTPException(
                403,
                f"Rol '{role}' no tiene permisos: {', '.join(missing)}"
            )
        return user

    return _check_permission
