"""
FACTURA-SV: Admin Panel Router
================================
Endpoints para gestión completa de la plataforma.
Solo accesible por usuarios con role "admin".
"""
from fastapi import Request,  APIRouter, Depends, HTTPException, Query
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


# ══════════════════════════════════════════════════════════
# WHATSAPP GLOBAL CONFIG (centralizado — envía desde cuenta FACTURA-SV)
# ══════════════════════════════════════════════════════════

@router.get("/whatsapp-config")
async def get_whatsapp_config(
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Get global WhatsApp config (admin only)."""
    result = supabase.table("platform_config").select("key, value").in_(
        "key", ["whatsapp_enabled", "whatsapp_phone_number_id", "whatsapp_waba_id"]
    ).execute()
    config = {r["key"]: r["value"] for r in (result.data or [])}
    return {
        "enabled": config.get("whatsapp_enabled", "false") == "true",
        "phone_number_id": config.get("whatsapp_phone_number_id", ""),
        "waba_id": config.get("whatsapp_waba_id", ""),
        "has_token": bool(config.get("whatsapp_phone_number_id")),
    }


@router.post("/whatsapp-config")
async def save_whatsapp_config(
    request: Request,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Save global WhatsApp config (admin only). Token encrypted."""
    from app.services.encryption_service import EncryptionService
    data = await request.json()

    updates = {
        "whatsapp_enabled": str(data.get("enabled", False)).lower(),
        "whatsapp_phone_number_id": data.get("phone_number_id", ""),
        "whatsapp_waba_id": data.get("waba_id", ""),
    }

    # Encrypt access token
    if data.get("access_token"):
        enc = EncryptionService()
        encrypted = enc.encrypt_string(data["access_token"], "platform_global")
        updates["whatsapp_access_token"] = encrypted

    for key, value in updates.items():
        supabase.table("platform_config").upsert(
            {"key": key, "value": value, "updated_at": "now()"},
            on_conflict="key"
        ).execute()

    return {"success": True, "message": "Configuración WhatsApp guardada"}


# ══════════════════════════════════════════════════════════
# MANUAL PLAN ACTIVATION (cash / transfer)
# ══════════════════════════════════════════════════════════

class ManualPlanActivation(BaseModel):
    plan: str = Field(..., description="Plan: basico, profesional, enterprise")
    payment_method: str = Field(..., description="Método: cash o transfer")
    months: int = Field(1, ge=1, le=12, description="Meses a activar")
    amount: float = Field(0, ge=0, description="Monto recibido en USD")
    reference: Optional[str] = Field(None, description="Nº referencia de transferencia")
    notes: Optional[str] = Field(None, description="Notas internas")


PLAN_QUOTAS = {
    "free": 50,
    "basico": 200,
    "profesional": 1000,
    "enterprise": 999999,
}

PLAN_PRICES = {
    "basico": 14.99,
    "profesional": 24.99,
    "enterprise": 39.99,
}


@router.post("/organizations/{org_id}/activate-plan")
async def activate_plan_manual(
    org_id: str,
    body: ManualPlanActivation,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Activar plan manualmente cuando el cliente paga por transferencia o efectivo.
    Setea plan, payment_method, plan_expires_at, monthly_quota, plan_status.
    """
    if body.plan not in PLAN_QUOTAS:
        raise HTTPException(400, f"Plan inválido. Opciones: {list(PLAN_QUOTAS.keys())}")
    if body.payment_method not in ("cash", "transfer"):
        raise HTTPException(400, "Método de pago debe ser 'cash' o 'transfer'")

    # Verify org exists
    org = db.table("organizations").select("id, name, plan, plan_status").eq("id", org_id).single().execute()
    if not org.data:
        raise HTTPException(404, "Organización no encontrada")

    # Calculate expiration
    from dateutil.relativedelta import relativedelta
    now = datetime.utcnow()
    expires_at = now + relativedelta(months=body.months)

    # Build payment note
    method_label = "Efectivo" if body.payment_method == "cash" else "Transferencia"
    note = (
        f"[{now.strftime('%Y-%m-%d %H:%M')}] "
        f"Plan {body.plan} activado por {body.months} mes(es) — "
        f"{method_label} ${body.amount:.2f}"
    )
    if body.reference:
        note += f" — Ref: {body.reference}"
    if body.notes:
        note += f" — {body.notes}"
    note += f" — Expira: {expires_at.strftime('%Y-%m-%d')}"

    # Append to existing notes
    existing_notes = org.data.get("payment_notes") or ""
    all_notes = f"{existing_notes}\n{note}".strip()

    # Update organization
    update_data = {
        "plan": body.plan,
        "plan_status": "active",
        "payment_method": body.payment_method,
        "plan_expires_at": expires_at.isoformat(),
        "monthly_quota": PLAN_QUOTAS[body.plan],
        "is_active": True,
        "payment_notes": all_notes,
        "updated_at": now.isoformat(),
    }

    result = db.table("organizations").update(update_data).eq("id", org_id).execute()
    if not result.data:
        raise HTTPException(500, "Error actualizando organización")

    # Log to audit if table exists
    try:
        db.table("audit_log").insert({
            "org_id": org_id,
            "user_id": admin.get("id"),
            "action": "manual_plan_activation",
            "entity_type": "organization",
            "entity_id": org_id,
            "details": {
                "plan": body.plan,
                "months": body.months,
                "payment_method": body.payment_method,
                "amount": body.amount,
                "reference": body.reference,
                "expires_at": expires_at.isoformat(),
            },
        }).execute()
    except Exception:
        pass  # audit_log might not exist yet

    return {
        "success": True,
        "message": f"Plan {body.plan} activado para {org.data['name']} hasta {expires_at.strftime('%Y-%m-%d')}",
        "data": {
            "org_id": org_id,
            "org_name": org.data["name"],
            "plan": body.plan,
            "payment_method": body.payment_method,
            "months": body.months,
            "amount": body.amount,
            "expires_at": expires_at.isoformat(),
            "monthly_quota": PLAN_QUOTAS[body.plan],
        },
    }


@router.get("/manual-payments")
async def list_manual_payments(
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """Lista organizaciones con pago manual activo (cash/transfer)."""
    result = db.table("organizations").select(
        "id, name, plan, plan_status, payment_method, plan_expires_at, payment_notes, updated_at"
    ).in_("payment_method", ["cash", "transfer"]).order("updated_at", desc=True).execute()

    # Add expiration status
    now = datetime.utcnow()
    enriched = []
    for org in (result.data or []):
        exp = org.get("plan_expires_at")
        if exp:
            exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00")).replace(tzinfo=None)
            org["_is_expired"] = exp_dt < now
            org["_days_remaining"] = max(0, (exp_dt - now).days)
        else:
            org["_is_expired"] = False
            org["_days_remaining"] = None
        enriched.append(org)

    return {"data": enriched, "total": len(enriched)}
