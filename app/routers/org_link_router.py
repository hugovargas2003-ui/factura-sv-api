"""
FACTURA-SV: Organization Link Code Router
==========================================
Manages org link codes (FSV-XXXX-XXXX) for connecting
contadores/external users to organizations.

Endpoints:
  GET    /api/v1/org-link/code          - Get current link code for org
  POST   /api/v1/org-link/regenerate    - Regenerate link code
  POST   /api/v1/org-link/toggle        - Enable/disable link code
  POST   /api/v1/org-link/redeem        - Redeem a link code (join org)
  GET    /api/v1/org-link/members       - List connected members
  DELETE /api/v1/org-link/members/{uid} - Disconnect a member
  PATCH  /api/v1/org-link/members/{uid}/role - Change member role
  POST   /api/v1/org-link/confirm/{uid} - Confirm a pending connection
"""

import logging
import secrets
import string
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.dependencies import get_current_user, get_supabase
from supabase import Client as SupabaseClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/org-link", tags=["org-link"])


def _generate_link_code() -> str:
    """Generate a code like FSV-XXXX-XXXX (alphanumeric uppercase)."""
    chars = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(chars) for _ in range(4))
    part2 = "".join(secrets.choice(chars) for _ in range(4))
    return f"FSV-{part1}-{part2}"


# ── Request/Response Models ──

class RedeemRequest(BaseModel):
    code: str

class ToggleRequest(BaseModel):
    enabled: bool

class RoleUpdate(BaseModel):
    role: str  # "member", "admin", "contador"


# ── Helpers ──

async def _require_owner_or_admin(user: dict, db: SupabaseClient):
    """Verify user is owner or admin of their current org."""
    membership = db.table("user_organizations").select(
        "role"
    ).eq("user_id", user["user_id"]).eq("org_id", user["org_id"]).execute()

    if not membership.data:
        raise HTTPException(403, "No pertenece a esta organizacion")

    role = membership.data[0]["role"]
    if role not in ("owner", "admin"):
        raise HTTPException(403, "Solo el owner o admin puede gestionar conexiones")

    return role


# ═══════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════


@router.get("/code")
async def get_link_code(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Get current org link code. Creates one if none exists."""
    await _require_owner_or_admin(user, db)

    org = db.table("organizations").select(
        "id, name, link_code, link_code_enabled"
    ).eq("id", user["org_id"]).single().execute()

    if not org.data:
        raise HTTPException(404, "Organizacion no encontrada")

    # Auto-generate if no code exists
    if not org.data.get("link_code"):
        code = _generate_link_code()
        db.table("organizations").update({
            "link_code": code,
            "link_code_enabled": True,
        }).eq("id", user["org_id"]).execute()
        return {
            "code": code,
            "enabled": True,
            "org_name": org.data["name"],
        }

    return {
        "code": org.data["link_code"],
        "enabled": org.data.get("link_code_enabled", True),
        "org_name": org.data["name"],
    }


@router.post("/regenerate")
async def regenerate_code(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Regenerate the link code (invalidates the old one)."""
    await _require_owner_or_admin(user, db)

    code = _generate_link_code()
    db.table("organizations").update({
        "link_code": code,
        "link_code_enabled": True,
    }).eq("id", user["org_id"]).execute()

    logger.info(f"Link code regenerated for org {user['org_id'][:8]}...")
    return {"code": code, "enabled": True}


@router.post("/toggle")
async def toggle_code(
    body: ToggleRequest,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Enable or disable the link code."""
    await _require_owner_or_admin(user, db)

    db.table("organizations").update({
        "link_code_enabled": body.enabled,
    }).eq("id", user["org_id"]).execute()

    return {"enabled": body.enabled}


@router.post("/redeem")
async def redeem_code(
    body: RedeemRequest,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Redeem a link code to join an organization."""
    code = body.code.strip().upper()

    # Find org by code
    org = db.table("organizations").select(
        "id, name, link_code_enabled"
    ).eq("link_code", code).execute()

    if not org.data:
        raise HTTPException(404, "Codigo de vinculacion invalido")

    target_org = org.data[0]

    if not target_org.get("link_code_enabled", True):
        raise HTTPException(400, "Este codigo esta desactivado por el dueño de la empresa")

    target_org_id = target_org["id"]

    # Don't allow joining own org
    if target_org_id == user["org_id"]:
        raise HTTPException(400, "Ya pertenece a esta organizacion")

    # Check if already a member
    existing = db.table("user_organizations").select(
        "id"
    ).eq("user_id", user["user_id"]).eq("org_id", target_org_id).execute()

    if existing.data:
        raise HTTPException(400, "Ya esta vinculado a esta organizacion")

    # Add membership with confirmed_by_owner = false
    db.table("user_organizations").insert({
        "user_id": user["user_id"],
        "org_id": target_org_id,
        "role": "contador",
        "is_default": False,
        "confirmed_by_owner": False,
    }).execute()

    logger.info(
        f"User {user['user_id'][:8]} redeemed code for org {target_org_id[:8]} "
        f"(pending confirmation)"
    )

    return {
        "success": True,
        "org_id": target_org_id,
        "org_name": target_org["name"],
        "status": "pending_confirmation",
        "message": f"Vinculado a {target_org['name']}. El dueño debe confirmar su conexion.",
    }


@router.get("/members")
async def list_members(
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """List all members connected to this org (for owner/admin)."""
    await _require_owner_or_admin(user, db)

    memberships = db.table("user_organizations").select(
        "user_id, role, is_default, confirmed_by_owner, created_at"
    ).eq("org_id", user["org_id"]).execute()

    if not memberships.data:
        return {"members": []}

    # Get user details
    user_ids = [m["user_id"] for m in memberships.data]
    users = db.table("users").select(
        "id, email, full_name"
    ).in_("id", user_ids).execute()

    user_map = {u["id"]: u for u in (users.data or [])}

    members = []
    for m in memberships.data:
        u = user_map.get(m["user_id"], {})
        members.append({
            "user_id": m["user_id"],
            "email": u.get("email", ""),
            "full_name": u.get("full_name", ""),
            "role": m["role"],
            "confirmed": m.get("confirmed_by_owner", True),
            "joined_at": m["created_at"],
            "is_self": m["user_id"] == user["user_id"],
        })

    # Sort: unconfirmed first, then by role
    role_order = {"owner": 0, "admin": 1, "member": 2, "contador": 3}
    members.sort(key=lambda x: (
        x["confirmed"],  # False first (pending)
        role_order.get(x["role"], 99),
    ))

    return {"members": members}


@router.delete("/members/{target_user_id}")
async def disconnect_member(
    target_user_id: str,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Disconnect (remove) a member from the org."""
    caller_role = await _require_owner_or_admin(user, db)

    # Can't remove yourself via this endpoint
    if target_user_id == user["user_id"]:
        raise HTTPException(400, "No puede desconectarse a si mismo. Use la opcion de salir de la organizacion.")

    # Check target membership
    membership = db.table("user_organizations").select(
        "role"
    ).eq("user_id", target_user_id).eq("org_id", user["org_id"]).execute()

    if not membership.data:
        raise HTTPException(404, "Usuario no encontrado en esta organizacion")

    target_role = membership.data[0]["role"]

    # Admin can't remove owner
    if target_role == "owner" and caller_role != "owner":
        raise HTTPException(403, "Solo un owner puede remover a otro owner")

    # Remove membership
    db.table("user_organizations").delete().eq(
        "user_id", target_user_id
    ).eq("org_id", user["org_id"]).execute()

    # If this was their active org, switch to another
    user_data = db.table("users").select("org_id").eq(
        "id", target_user_id
    ).single().execute()

    if user_data.data and user_data.data["org_id"] == user["org_id"]:
        other = db.table("user_organizations").select(
            "org_id"
        ).eq("user_id", target_user_id).limit(1).execute()

        if other.data:
            db.table("users").update({
                "org_id": other.data[0]["org_id"]
            }).eq("id", target_user_id).execute()

    logger.info(
        f"User {target_user_id[:8]} disconnected from org {user['org_id'][:8]} "
        f"by {user['user_id'][:8]}"
    )

    return {"success": True, "message": "Usuario desconectado"}


@router.patch("/members/{target_user_id}/role")
async def change_member_role(
    target_user_id: str,
    body: RoleUpdate,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Change a member's role."""
    caller_role = await _require_owner_or_admin(user, db)

    if body.role not in ("member", "admin", "contador"):
        raise HTTPException(400, "Rol invalido. Opciones: member, admin, contador")

    if target_user_id == user["user_id"]:
        raise HTTPException(400, "No puede cambiar su propio rol")

    # Verify target exists
    membership = db.table("user_organizations").select(
        "role"
    ).eq("user_id", target_user_id).eq("org_id", user["org_id"]).execute()

    if not membership.data:
        raise HTTPException(404, "Usuario no encontrado en esta organizacion")

    # Only owner can change roles
    if caller_role != "owner":
        raise HTTPException(403, "Solo el owner puede cambiar roles")

    db.table("user_organizations").update({
        "role": body.role,
    }).eq("user_id", target_user_id).eq("org_id", user["org_id"]).execute()

    return {"success": True, "role": body.role}


@router.post("/confirm/{target_user_id}")
async def confirm_member(
    target_user_id: str,
    user: dict = Depends(get_current_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Confirm a pending connection (set confirmed_by_owner = true)."""
    await _require_owner_or_admin(user, db)

    # Verify target exists and is unconfirmed
    membership = db.table("user_organizations").select(
        "confirmed_by_owner"
    ).eq("user_id", target_user_id).eq("org_id", user["org_id"]).execute()

    if not membership.data:
        raise HTTPException(404, "Usuario no encontrado en esta organizacion")

    if membership.data[0].get("confirmed_by_owner", True):
        return {"success": True, "message": "Ya estaba confirmado"}

    db.table("user_organizations").update({
        "confirmed_by_owner": True,
    }).eq("user_id", target_user_id).eq("org_id", user["org_id"]).execute()

    logger.info(
        f"User {target_user_id[:8]} confirmed in org {user['org_id'][:8]} "
        f"by {user['user_id'][:8]}"
    )

    return {"success": True, "message": "Conexion confirmada"}
