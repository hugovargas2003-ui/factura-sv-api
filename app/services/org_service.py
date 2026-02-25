"""
org_service.py — Multi-Organization management.

Location: app/services/org_service.py
NEW FILE — does not modify any existing infrastructure.

Handles:
- List organizations for a user
- Switch active organization
- Add user to organization (invite)
"""

from typing import Any


async def list_user_organizations(supabase: Any, user_id: str) -> list[dict]:
    """
    List all organizations a user belongs to,
    with the active org marked.
    """
    # Get current active org_id
    user_result = supabase.table("users").select(
        "org_id"
    ).eq("id", user_id).single().execute()
    
    current_org_id = user_result.data["org_id"] if user_result.data else None

    # Get all memberships
    memberships = supabase.table("user_organizations").select(
        "org_id, role, is_default, created_at"
    ).eq("user_id", user_id).execute()

    if not memberships.data:
        return []

    # Fetch org details for each membership
    org_ids = [m["org_id"] for m in memberships.data]
    orgs = supabase.table("organizations").select(
        "id, name, nit, plan, plan_status"
    ).in_("id", org_ids).execute()

    org_map = {o["id"]: o for o in (orgs.data or [])}

    result = []
    for m in memberships.data:
        org = org_map.get(m["org_id"], {})
        result.append({
            "org_id": m["org_id"],
            "name": org.get("name", "—"),
            "nit": org.get("nit", ""),
            "plan": org.get("plan", "free"),
            "role": m["role"],
            "is_default": m["is_default"],
            "is_active": m["org_id"] == current_org_id,
        })

    # Sort: active first, then default, then alphabetical
    result.sort(key=lambda x: (not x["is_active"], not x["is_default"], x["name"]))
    return result


async def switch_organization(
    supabase: Any, user_id: str, target_org_id: str
) -> dict:
    """
    Switch user's active organization.
    Validates membership before switching.
    """
    # Verify user has access to target org
    membership = supabase.table("user_organizations").select(
        "id, role"
    ).eq("user_id", user_id).eq("org_id", target_org_id).execute()

    if not membership.data:
        raise ValueError("No tiene acceso a esa organización")

    # Update users.org_id (the active org pointer)
    supabase.table("users").update({
        "org_id": target_org_id
    }).eq("id", user_id).execute()

    # Get org name for response
    org = supabase.table("organizations").select(
        "name, nit, plan"
    ).eq("id", target_org_id).single().execute()

    return {
        "success": True,
        "org_id": target_org_id,
        "name": org.data.get("name", "") if org.data else "",
        "role": membership.data[0]["role"],
    }


async def add_user_to_organization(
    supabase: Any,
    admin_org_id: str,
    target_email: str,
    role: str = "member",
) -> dict:
    """
    Add an existing user to an organization.
    The user must already have a Supabase auth account.
    """
    # Find user by email
    user_result = supabase.table("users").select(
        "id, email, full_name"
    ).eq("email", target_email).execute()

    if not user_result.data:
        raise ValueError(f"Usuario con email {target_email} no encontrado")

    target_user = user_result.data[0]
    target_user_id = target_user["id"]

    # Check not already member
    existing = supabase.table("user_organizations").select(
        "id"
    ).eq("user_id", target_user_id).eq("org_id", admin_org_id).execute()

    if existing.data:
        raise ValueError("El usuario ya pertenece a esta organización")

    # Insert membership
    supabase.table("user_organizations").insert({
        "user_id": target_user_id,
        "org_id": admin_org_id,
        "role": role,
        "is_default": False,
    }).execute()

    return {
        "success": True,
        "user_id": target_user_id,
        "email": target_user["email"],
        "full_name": target_user.get("full_name", ""),
        "role": role,
    }


async def remove_user_from_organization(
    supabase: Any,
    org_id: str,
    target_user_id: str,
) -> dict:
    """Remove a user from an organization."""
    # Don't allow removing the last owner
    owners = supabase.table("user_organizations").select(
        "id"
    ).eq("org_id", org_id).eq("role", "owner").execute()

    membership = supabase.table("user_organizations").select(
        "role"
    ).eq("user_id", target_user_id).eq("org_id", org_id).execute()

    if not membership.data:
        raise ValueError("El usuario no pertenece a esta organización")

    if membership.data[0]["role"] == "owner" and len(owners.data or []) <= 1:
        raise ValueError("No se puede remover al único owner de la organización")

    # Remove membership
    supabase.table("user_organizations").delete().eq(
        "user_id", target_user_id
    ).eq("org_id", org_id).execute()

    # If this was their active org, switch to another
    user_data = supabase.table("users").select("org_id").eq(
        "id", target_user_id
    ).single().execute()

    if user_data.data and user_data.data["org_id"] == org_id:
        other = supabase.table("user_organizations").select(
            "org_id"
        ).eq("user_id", target_user_id).limit(1).execute()

        if other.data:
            supabase.table("users").update({
                "org_id": other.data[0]["org_id"]
            }).eq("id", target_user_id).execute()

    return {"success": True}
