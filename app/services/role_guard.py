"""
FACTURA-SV: Role Guard (T2-04)
================================
Middleware para restringir endpoints por rol.
Roles: owner > admin > member > viewer
"""
from fastapi import HTTPException

ROLE_HIERARCHY = {"owner": 4, "admin": 3, "member": 2, "viewer": 1}


def require_role(user: dict, minimum_role: str):
    """Raises 403 if user role is below minimum_role."""
    user_level = ROLE_HIERARCHY.get(user.get("role", "viewer"), 0)
    required_level = ROLE_HIERARCHY.get(minimum_role, 99)
    if user_level < required_level:
        raise HTTPException(
            status_code=403,
            detail=f"Permiso insuficiente. Se requiere rol '{minimum_role}' o superior."
        )


def require_owner(user: dict):
    require_role(user, "owner")


def require_admin(user: dict):
    require_role(user, "admin")


def require_member(user: dict):
    require_role(user, "member")


def get_role_permissions(role: str) -> dict:
    """Returns permission map for a role."""
    perms = {
        "viewer": {
            "can_view_dtes": True, "can_emit_dte": False, "can_manage_config": False,
            "can_manage_users": False, "can_view_reports": True, "can_manage_products": False,
            "can_manage_sucursales": False, "can_manage_inventory": False, "can_batch_emit": False,
        },
        "member": {
            "can_view_dtes": True, "can_emit_dte": True, "can_manage_config": False,
            "can_manage_users": False, "can_view_reports": True, "can_manage_products": True,
            "can_manage_sucursales": False, "can_manage_inventory": True, "can_batch_emit": True,
        },
        "admin": {
            "can_view_dtes": True, "can_emit_dte": True, "can_manage_config": True,
            "can_manage_users": True, "can_view_reports": True, "can_manage_products": True,
            "can_manage_sucursales": True, "can_manage_inventory": True, "can_batch_emit": True,
        },
        "owner": {
            "can_view_dtes": True, "can_emit_dte": True, "can_manage_config": True,
            "can_manage_users": True, "can_view_reports": True, "can_manage_products": True,
            "can_manage_sucursales": True, "can_manage_inventory": True, "can_batch_emit": True,
        },
    }
    return perms.get(role, perms["viewer"])
