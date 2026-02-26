"""
FACTURA-SV: Platform Config Admin Router
==========================================
Admin endpoints to read/write platform configuration.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Any
from supabase import Client as SupabaseClient

from app.dependencies import get_current_user, get_supabase
from app.services.platform_config import (
    get_all_config,
    get_config_category,
    set_config,
    set_config_bulk,
    get_bank_info_from_config,
)

router = APIRouter(prefix="/config", tags=["config"])


async def require_admin(user: dict = Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "Se requieren permisos de administrador")
    return user


class ConfigUpdate(BaseModel):
    key: str
    value: Any


class ConfigBulkUpdate(BaseModel):
    updates: dict[str, Any]


@router.get("/all")
async def get_all_platform_config(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Get all platform config entries (admin only)."""
    configs = await get_all_config(db)
    grouped: dict[str, list] = {}
    for c in configs:
        cat = c.get("category", "general")
        if cat not in grouped:
            grouped[cat] = []
        if c.get("is_secret") and c.get("value"):
            c["value"] = "***"
        grouped[cat].append(c)
    return {"success": True, "data": grouped, "total": len(configs)}


@router.get("/category/{category}")
async def get_category_config(
    category: str,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Get all config for a specific category."""
    cfg = await get_config_category(db, category)
    return {"success": True, "data": cfg}


@router.put("/update")
async def update_config_endpoint(
    body: ConfigUpdate,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Update a single config value."""
    ok = await set_config(db, body.key, body.value, admin.get("id"))
    if not ok:
        raise HTTPException(404, f"Config key '{body.key}' no encontrada")
    return {"success": True, "message": f"Config '{body.key}' actualizada"}


@router.put("/bulk")
async def update_config_bulk_endpoint(
    body: ConfigBulkUpdate,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Update multiple config values at once."""
    count = await set_config_bulk(db, body.updates, admin.get("id"))
    return {"success": True, "message": f"{count} configuraciones actualizadas", "updated": count}


@router.get("/bank-info-public")
async def get_public_bank_info(
    db: SupabaseClient = Depends(get_supabase),
):
    """Public endpoint: bank info for transfers (no auth needed)."""
    bank = await get_bank_info_from_config(db)
    return {"success": True, "data": bank}
