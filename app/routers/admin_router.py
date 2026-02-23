"""
FACTURA-SV: Admin Panel Router
================================
Endpoints para gestión completa de la plataforma.
Solo accesible por usuarios con role "admin".
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

from app.dependencies import get_current_user, get_supabase
from supabase import Client as SupabaseClient

router = APIRouter(prefix="/admin", tags=["admin"])


# ── Admin Guard ──

async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    """Solo permite acceso a usuarios con role 'admin'."""
    if user.get("role") not in ("admin",):
        raise HTTPException(403, "Acceso denegado: se requiere rol de administrador")
    return user


# ── Schemas ──

class OrgUpdate(BaseModel):
    name: Optional[str] = None
    nit: Optional[str] = None
    plan: Optional[str] = None
    monthly_quota: Optional[int] = None
    plan_status: Optional[str] = None
    max_companies: Optional[int] = None
    stripe_customer_id: Optional[str] = None
    stripe_subscription_id: Optional[str] = None


class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    org_id: Optional[str] = None


# ═══════════════════════════════════════════
# DASHBOARD / STATS
# ═══════════════════════════════════════════

@router.get("/stats")
async def get_platform_stats(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Estadísticas globales de la plataforma."""
    orgs = db.table("organizations").select("id", count="exact").execute()
    users = db.table("users").select("id", count="exact").execute()
    dtes = db.table("dtes").select("id", count="exact").execute()
    invoices = db.table("invoices").select("id", count="exact").execute()
    credentials = db.table("dte_credentials").select("id", count="exact").execute()

    # Orgs por plan
    all_orgs = db.table("organizations").select("plan, plan_status").execute().data
    plans = {}
    statuses = {}
    for o in all_orgs:
        p = o.get("plan", "unknown")
        s = o.get("plan_status", "unknown")
        plans[p] = plans.get(p, 0) + 1
        statuses[s] = statuses.get(s, 0) + 1

    # DTEs por estado
    all_dtes = db.table("dtes").select("estado, tipo_dte").execute().data
    dte_by_status = {}
    dte_by_type = {}
    for d in all_dtes:
        st = d.get("estado", "unknown")
        tp = d.get("tipo_dte", "unknown")
        dte_by_status[st] = dte_by_status.get(st, 0) + 1
        dte_by_type[tp] = dte_by_type.get(tp, 0) + 1

    return {
        "totals": {
            "organizations": orgs.count or 0,
            "users": users.count or 0,
            "dtes": dtes.count or 0,
            "invoices": invoices.count or 0,
            "credentials_configured": credentials.count or 0,
        },
        "orgs_by_plan": plans,
        "orgs_by_status": statuses,
        "dtes_by_status": dte_by_status,
        "dtes_by_type": dte_by_type,
    }


# ═══════════════════════════════════════════
# ORGANIZATIONS
# ═══════════════════════════════════════════

@router.get("/organizations")
async def list_organizations(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
    search: Optional[str] = Query(None, description="Buscar por nombre o NIT"),
    plan: Optional[str] = Query(None, description="Filtrar por plan"),
    status: Optional[str] = Query(None, description="Filtrar por estado"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista todas las organizaciones con estadísticas."""
    query = db.table("organizations").select("*", count="exact")

    if search:
        query = query.or_(f"name.ilike.%{search}%,nit.ilike.%{search}%")
    if plan:
        query = query.eq("plan", plan)
    if status:
        query = query.eq("plan_status", status)

    result = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()

    # Enrich with user count and DTE count per org
    org_ids = [o["id"] for o in result.data]
    enriched = []
    for org in result.data:
        oid = org["id"]
        u_count = db.table("users").select("id", count="exact").eq("org_id", oid).execute().count or 0
        d_count = db.table("dtes").select("id", count="exact").eq("org_id", oid).execute().count or 0
        cred = db.table("dte_credentials").select("id").eq("org_id", oid).limit(1).execute().data
        org["_user_count"] = u_count
        org["_dte_count"] = d_count
        org["_has_credentials"] = len(cred) > 0
        enriched.append(org)

    return {"data": enriched, "total": result.count or 0, "limit": limit, "offset": offset}


@router.get("/organizations/{org_id}")
async def get_organization(
    org_id: str,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Detalle de una organización con usuarios y DTEs recientes."""
    org = db.table("organizations").select("*").eq("id", org_id).single().execute()
    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    users = db.table("users").select("*").eq("org_id", org_id).order("created_at").execute().data
    dtes = db.table("dtes").select("*").eq("org_id", org_id).order("created_at", desc=True).limit(20).execute().data
    creds = db.table("dte_credentials").select("id, created_at").eq("org_id", org_id).execute().data
    invoices = db.table("invoices").select("*").eq("org_id", org_id).order("created_at", desc=True).limit(10).execute().data

    return {
        "organization": org.data,
        "users": users,
        "recent_dtes": dtes,
        "credentials": creds,
        "recent_invoices": invoices,
    }


@router.patch("/organizations/{org_id}")
async def update_organization(
    org_id: str,
    updates: OrgUpdate,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Editar datos de una organización."""
    data = {k: v for k, v in updates.model_dump().items() if v is not None}
    if not data:
        raise HTTPException(400, "No hay campos para actualizar")

    data["updated_at"] = datetime.utcnow().isoformat()
    result = db.table("organizations").update(data).eq("id", org_id).execute()
    if not result.data:
        raise HTTPException(404, "Organización no encontrada")
    return {"success": True, "data": result.data[0]}


@router.patch("/organizations/{org_id}/toggle-status")
async def toggle_org_status(
    org_id: str,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Activar/desactivar una organización."""
    org = db.table("organizations").select("plan_status").eq("id", org_id).single().execute()
    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    new_status = "inactive" if org.data["plan_status"] == "active" else "active"
    result = db.table("organizations").update({
        "plan_status": new_status,
        "updated_at": datetime.utcnow().isoformat()
    }).eq("id", org_id).execute()

    return {"success": True, "new_status": new_status, "data": result.data[0]}


# ═══════════════════════════════════════════
# USERS
# ═══════════════════════════════════════════

@router.get("/users")
async def list_users(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
    search: Optional[str] = Query(None, description="Buscar por email o nombre"),
    role: Optional[str] = Query(None, description="Filtrar por rol"),
    org_id: Optional[str] = Query(None, description="Filtrar por organización"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista todos los usuarios de la plataforma."""
    query = db.table("users").select("*", count="exact")

    if search:
        query = query.or_(f"email.ilike.%{search}%,full_name.ilike.%{search}%")
    if role:
        query = query.eq("role", role)
    if org_id:
        query = query.eq("org_id", org_id)

    result = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()

    # Enrich with org name
    enriched = []
    org_cache = {}
    for u in result.data:
        oid = u.get("org_id")
        if oid and oid not in org_cache:
            org_r = db.table("organizations").select("name, plan").eq("id", oid).single().execute()
            org_cache[oid] = org_r.data if org_r.data else {}
        u["_org_name"] = org_cache.get(oid, {}).get("name", "—")
        u["_org_plan"] = org_cache.get(oid, {}).get("plan", "—")
        enriched.append(u)

    return {"data": enriched, "total": result.count or 0, "limit": limit, "offset": offset}


@router.patch("/users/{user_id}")
async def update_user(
    user_id: str,
    updates: UserUpdate,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Editar datos de un usuario."""
    data = {k: v for k, v in updates.model_dump().items() if v is not None}
    if not data:
        raise HTTPException(400, "No hay campos para actualizar")

    result = db.table("users").update(data).eq("id", user_id).execute()
    if not result.data:
        raise HTTPException(404, "Usuario no encontrado")
    return {"success": True, "data": result.data[0]}


# ═══════════════════════════════════════════
# DTEs (cross-org)
# ═══════════════════════════════════════════

@router.get("/dtes")
async def list_all_dtes(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
    search: Optional[str] = Query(None, description="Buscar por código generación o número control"),
    tipo_dte: Optional[str] = Query(None, description="Filtrar por tipo"),
    estado: Optional[str] = Query(None, description="Filtrar por estado"),
    org_id: Optional[str] = Query(None, description="Filtrar por organización"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista todos los DTEs de toda la plataforma."""
    query = db.table("dtes").select("*", count="exact")

    if search:
        query = query.or_(f"codigo_generacion.ilike.%{search}%,numero_control.ilike.%{search}%")
    if tipo_dte:
        query = query.eq("tipo_dte", tipo_dte)
    if estado:
        query = query.eq("estado", estado)
    if org_id:
        query = query.eq("org_id", org_id)

    result = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return {"data": result.data, "total": result.count or 0, "limit": limit, "offset": offset}


# ═══════════════════════════════════════════
# INVOICES (cross-org)
# ═══════════════════════════════════════════

@router.get("/invoices")
async def list_all_invoices(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
    org_id: Optional[str] = Query(None, description="Filtrar por organización"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista todas las facturas subidas en la plataforma."""
    query = db.table("invoices").select("*", count="exact")
    if org_id:
        query = query.eq("org_id", org_id)

    result = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return {"data": result.data, "total": result.count or 0, "limit": limit, "offset": offset}
