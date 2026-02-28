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


# ═══════════════════════════════════════════════════════════
# ADMIN CREATE — Organizations & Users from field
# ═══════════════════════════════════════════════════════════

class AdminCreateOrg(BaseModel):
    """Create organization + optional owner user."""
    name: str = Field(..., min_length=2, max_length=200)
    nit: Optional[str] = Field(None, max_length=20)
    nrc: Optional[str] = Field(None, max_length=20)
    plan: str = Field("free", pattern="^(free|emprendedor|profesional|contador|enterprise)$")
    # Optional: create owner user at same time
    owner_email: Optional[str] = Field(None, description="Email del dueño — crea cuenta automáticamente")
    owner_name: Optional[str] = Field(None, description="Nombre completo del dueño")
    owner_password: Optional[str] = Field(None, min_length=6, description="Contraseña temporal")
    # Payment info
    payment_method: Optional[str] = Field("free", pattern="^(free|cash|transfer|stripe)$")
    months: Optional[int] = Field(None, ge=1, le=36)
    amount: Optional[float] = Field(None, ge=0)
    admin_notes: Optional[str] = None


class AdminCreateUser(BaseModel):
    """Create user and assign to existing organization."""
    email: str = Field(..., min_length=5, max_length=200)
    full_name: str = Field(..., min_length=2, max_length=200)
    password: str = Field(..., min_length=6, max_length=100, description="Contraseña temporal")
    org_id: str = Field(..., description="UUID de la organización")
    role: str = Field("admin", pattern="^(admin|member|viewer)$")


PLAN_DTE_LIMITS = {
    "free": 50,
    "emprendedor": 200,
    "profesional": 1000,
    "contador": 5000,
    "enterprise": 999999,
}


@router.post("/organizations/create")
async def admin_create_organization(
    body: AdminCreateOrg,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Admin creates organization from field.
    Optionally creates owner user with Supabase Auth account.
    
    Flow:
    1. Create organization row
    2. If owner_email provided: create Supabase Auth user → users row → user_organizations
    3. If paid plan: set expiration
    """
    import uuid
    from datetime import timedelta

    now = datetime.utcnow()

    # 1. Check NIT uniqueness if provided
    if body.nit:
        existing = db.table("organizations").select("id").eq("nit", body.nit).execute()
        if existing.data:
            raise HTTPException(400, f"Ya existe una organización con NIT {body.nit}")

    # 2. Calculate plan expiration
    expires_at = None
    if body.plan != "free" and body.months:
        expires_at = (now + timedelta(days=body.months * 30)).isoformat()

    # 3. Create organization
    org_data = {
        "name": body.name,
        "nit": body.nit or "",
        "nrc": body.nrc or "",
        "plan": body.plan,
        "plan_status": "active",
        "is_active": True,
        "payment_method": body.payment_method or "free",
        "monthly_quota": PLAN_DTE_LIMITS.get(body.plan, 50),
        "max_companies": 999,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }

    if expires_at:
        org_data["plan_expires_at"] = expires_at
        org_data["plan_started_at"] = now.isoformat()
        org_data["plan_months"] = body.months

    if body.amount:
        method_label = {"cash": "Efectivo", "transfer": "Transferencia"}.get(body.payment_method, body.payment_method)
        org_data["payment_notes"] = (
            f"[{now.strftime('%Y-%m-%d')}] Creado por admin. "
            f"{method_label} ${body.amount:.2f} x{body.months or 0}m. "
            f"{body.admin_notes or ''}"
        ).strip()
    elif body.admin_notes:
        org_data["payment_notes"] = body.admin_notes

    org_result = db.table("organizations").insert(org_data).execute()
    if not org_result.data:
        raise HTTPException(500, "Error creando organización")

    org_id = org_result.data[0]["id"]
    response = {
        "success": True,
        "organization": org_result.data[0],
        "user_created": False,
    }

    # 4. Create owner user if email provided
    if body.owner_email:
        try:
            user_result = _admin_create_auth_user(
                db=db,
                email=body.owner_email,
                password=body.owner_password or "FacSV2026!",
                full_name=body.owner_name or body.name,
                org_id=org_id,
                role="admin",
            )
            response["user_created"] = True
            response["user"] = user_result
        except Exception as e:
            response["user_error"] = str(e)
            response["user_created"] = False

    # 5. Log payment if paid plan
    if body.plan != "free" and body.amount and body.amount > 0:
        try:
            db.table("manual_payments").insert({
                "org_id": org_id,
                "payment_method": body.payment_method or "cash",
                "amount": body.amount,
                "plan": body.plan,
                "months": body.months or 1,
                "status": "active",
                "transfer_verified": True,
                "verified_by": admin.get("email", "admin"),
                "verified_at": now.isoformat(),
                "period_start": now.isoformat(),
                "period_end": expires_at,
                "admin_notes": f"Creado desde admin panel. {body.admin_notes or ''}".strip(),
            }).execute()
        except Exception:
            pass  # Non-blocking

    return response


@router.post("/users/create")
async def admin_create_user(
    body: AdminCreateUser,
    admin: dict = Depends(require_admin),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Admin creates a user with Supabase Auth account and assigns to org.
    
    Flow:
    1. Verify org exists
    2. Check email not already registered
    3. Create Supabase Auth user (email_confirm=True to skip verification)
    4. Insert into users table
    5. Insert into user_organizations table
    """
    # 1. Verify org exists
    org = db.table("organizations").select("id, name").eq("id", body.org_id).single().execute()
    if not org.data:
        raise HTTPException(404, f"Organización {body.org_id} no encontrada")

    # 2. Check email not already registered
    existing = db.table("users").select("id").eq("email", body.email).execute()
    if existing.data:
        raise HTTPException(400, f"Ya existe un usuario con email {body.email}")

    # 3. Create auth user + users row + membership
    try:
        result = _admin_create_auth_user(
            db=db,
            email=body.email,
            password=body.password,
            full_name=body.full_name,
            org_id=body.org_id,
            role=body.role,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error creando usuario: {str(e)}")

    return {
        "success": True,
        "message": f"Usuario {body.email} creado y asignado a {org.data['name']}",
        "user": result,
    }


def _admin_create_auth_user(
    db: SupabaseClient,
    email: str,
    password: str,
    full_name: str,
    org_id: str,
    role: str = "admin",
) -> dict:
    """
    Helper: create Supabase Auth user + users row + user_organizations.
    Uses service_role so no email verification needed.
    """
    # Create Supabase Auth user (service role = auto-confirmed)
    try:
        auth_response = db.auth.admin.create_user({
            "email": email,
            "password": password,
            "email_confirm": True,
            "user_metadata": {
                "full_name": full_name,
            },
        })
        auth_user = auth_response.user
        if not auth_user:
            raise HTTPException(500, "Supabase Auth no retornó usuario")
        user_id = str(auth_user.id)
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        if "already been registered" in error_msg or "already exists" in error_msg:
            raise HTTPException(400, f"El email {email} ya está registrado en Auth")
        raise HTTPException(500, f"Error creando Auth user: {error_msg}")

    now = datetime.utcnow().isoformat()

    # Insert into users table
    try:
        db.table("users").insert({
            "id": user_id,
            "email": email,
            "full_name": full_name,
            "org_id": org_id,
            "role": role,
            "created_at": now,
        }).execute()
    except Exception as e:
        # Rollback: delete auth user if users insert fails
        try:
            db.auth.admin.delete_user(user_id)
        except Exception:
            pass
        raise HTTPException(500, f"Error insertando en tabla users: {str(e)}")

    # Insert user_organizations membership
    try:
        db.table("user_organizations").insert({
            "user_id": user_id,
            "org_id": org_id,
            "role": role,
            "is_default": True,
        }).execute()
    except Exception as e:
        # Non-blocking — user can still function without this
        pass

    return {
        "user_id": user_id,
        "email": email,
        "full_name": full_name,
        "org_id": org_id,
        "role": role,
        "password_set": True,
        "note": "El usuario puede iniciar sesión inmediatamente con su email y contraseña.",
    }


# ══════════════════════════════════════════════════════════
# ADMIN API KEY MANAGEMENT (for any org)
# ══════════════════════════════════════════════════════════

@router.post("/organizations/{org_id}/api-keys")
async def admin_create_api_key(
    org_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin generates API key for any org."""
    import secrets, hashlib
    data = await request.json()
    name = data.get("name", "Admin-generated")
    permissions = data.get("permissions", ["emit", "query"])

    raw = secrets.token_hex(32)
    full_key = f"fsv_live_{raw}"
    prefix = f"fsv_live_{raw[:8]}..."
    key_hash = hashlib.sha256(full_key.encode()).hexdigest()

    supabase.table("api_keys").insert({
        "org_id": org_id,
        "key_prefix": prefix,
        "key_hash": key_hash,
        "name": name,
        "permissions": permissions,
        "is_active": True,
        "created_by": "admin",
    }).execute()

    return {"api_key": full_key, "prefix": prefix, "name": name, "permissions": permissions}


@router.get("/organizations/{org_id}/api-keys")
async def admin_list_api_keys(
    org_id: str,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin lists API keys for any org."""
    result = supabase.table("api_keys").select(
        "id,key_prefix,name,permissions,is_active,last_used_at,created_by,created_at"
    ).eq("org_id", org_id).order("created_at", desc=True).execute()
    return {"api_keys": result.data or []}


@router.delete("/organizations/{org_id}/api-keys/{key_id}")
async def admin_delete_api_key(
    org_id: str, key_id: str,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin revokes API key for any org."""
    supabase.table("api_keys").delete().eq("id", key_id).eq("org_id", org_id).execute()
    return {"deleted": True, "id": key_id}


# ══════════════════════════════════════════════════════════
# ADMIN WHATSAPP PER-ORG MANAGEMENT
# ══════════════════════════════════════════════════════════

@router.get("/organizations/{org_id}/whatsapp")
async def admin_get_org_whatsapp(
    org_id: str,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin reads WhatsApp config for any org."""
    result = supabase.table("org_whatsapp_config").select(
        "id,phone_number_id,waba_id,display_phone,enabled,managed_by,"
        "notify_credits_low,notify_dte_emitido,notify_bienvenida,notify_cobranza,updated_at"
    ).eq("org_id", org_id).execute()
    if result.data:
        return result.data[0]
    return {"enabled": False, "managed_by": "none"}


@router.post("/organizations/{org_id}/whatsapp")
async def admin_save_org_whatsapp(
    org_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin configures WhatsApp for any org (managed service)."""
    from datetime import datetime, timezone
    data = await request.json()
    now = datetime.now(timezone.utc).isoformat()

    record = {
        "org_id": org_id,
        "phone_number_id": data.get("phone_number_id", ""),
        "waba_id": data.get("waba_id", ""),
        "display_phone": data.get("display_phone", ""),
        "enabled": data.get("enabled", False),
        "managed_by": "admin",
        "notify_credits_low": data.get("notify_credits_low", True),
        "notify_dte_emitido": data.get("notify_dte_emitido", False),
        "notify_bienvenida": data.get("notify_bienvenida", True),
        "notify_cobranza": data.get("notify_cobranza", False),
        "updated_at": now,
    }

    if data.get("access_token"):
        try:
            from app.services.encryption_service import EncryptionService
            enc = EncryptionService()
            record["access_token_encrypted"] = enc.encrypt_string(data["access_token"], org_id)
        except Exception:
            record["access_token_encrypted"] = data["access_token"]

    existing = supabase.table("org_whatsapp_config").select("id").eq("org_id", org_id).execute()
    if existing.data:
        supabase.table("org_whatsapp_config").update(record).eq("org_id", org_id).execute()
    else:
        record["created_at"] = now
        supabase.table("org_whatsapp_config").insert(record).execute()

    return {"success": True, "message": f"WhatsApp configurado para org {org_id}"}


# ══════════════════════════════════════════════════════════
# ADMIN CREDIT MANAGEMENT (add/deduct credits for any org)
# ══════════════════════════════════════════════════════════

@router.post("/organizations/{org_id}/credits")
async def admin_manage_credits(
    org_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin adds or deducts credits for any org."""
    from datetime import datetime, timezone
    data = await request.json()
    amount = int(data.get("amount", 0))
    reason = data.get("reason", "admin_adjustment")

    if amount == 0:
        raise HTTPException(400, "Amount must be non-zero")

    org = supabase.table("organizations").select("credit_balance").eq("id", org_id).single().execute()
    if not org.data:
        raise HTTPException(404, "Org not found")

    current = org.data["credit_balance"]
    new_balance = max(0, current + amount)

    supabase.table("organizations").update({"credit_balance": new_balance}).eq("id", org_id).execute()

    supabase.table("credit_transactions").insert({
        "org_id": org_id,
        "type": "purchase" if amount > 0 else "usage",
        "amount": amount,
        "balance": new_balance,
        "unit_price": 0,
        "total_paid": 0,
        "payment_ref": f"admin:{reason}",
    }).execute()

    return {"previous_balance": current, "adjustment": amount, "new_balance": new_balance, "reason": reason}


# ══════════════════════════════════════════════════════════
# ADMIN: CASH PAYMENT — Credits + Auto-Invoice
# ══════════════════════════════════════════════════════════

@router.post("/organizations/{org_id}/cash-payment")
async def admin_cash_payment(
    org_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """
    Admin registers a cash payment: credits added + auto-factura emitted.
    Body: {"cantidad": 500, "amount_received": 56.65, "payment_ref": "Efectivo en oficina", "metodo": "cash"}
    """
    from app.dependencies import get_encryption as _get_enc
    body = await request.json()
    cantidad = int(body.get("cantidad", 0))
    amount_received = float(body.get("amount_received", 0))
    payment_ref = body.get("payment_ref", f"cash_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
    metodo = body.get("metodo", "cash")  # cash | transfer | admin_grant

    if cantidad < 10:
        raise HTTPException(400, "Minimo 10 creditos")
    if amount_received < 0:
        raise HTTPException(400, "Monto invalido")

    # 1. Get current balance
    org = supabase.table("organizations").select("credit_balance, name").eq("id", org_id).single().execute()
    if not org.data:
        raise HTTPException(404, "Org not found")

    current = org.data["credit_balance"]
    new_balance = current + cantidad

    # 2. Update balance
    supabase.table("organizations").update({"credit_balance": new_balance}).eq("id", org_id).execute()

    # 3. Record transaction
    supabase.table("credit_transactions").insert({
        "org_id": org_id,
        "type": "purchase",
        "amount": cantidad,
        "balance": new_balance,
        "unit_price": round(amount_received / cantidad, 4) if cantidad > 0 else 0,
        "total_paid": amount_received,
        "payment_ref": payment_ref,
    }).execute()

    # 4. Auto-emit invoice (non-blocking)
    invoice_result = {"success": False, "error": "Not attempted"}
    if amount_received > 0:
        try:
            from app.services.auto_invoice_helper import emit_purchase_invoice
            from app.dependencies import EncryptionService
            encryption = EncryptionService()
            invoice_result = await emit_purchase_invoice(
                supabase=supabase,
                encryption=encryption,
                org_id=org_id,
                cantidad=cantidad,
                total_paid=amount_received,
                metodo_pago=metodo,
                payment_ref=payment_ref,
            )
        except Exception as e:
            invoice_result = {"success": False, "error": str(e)}

    return {
        "org_id": org_id,
        "org_name": org.data.get("name", ""),
        "previous_balance": current,
        "credits_added": cantidad,
        "new_balance": new_balance,
        "amount_received": amount_received,
        "payment_ref": payment_ref,
        "metodo": metodo,
        "invoice": invoice_result,
    }


@router.post("/organizations/{org_id}/verify-transfer")
async def admin_verify_transfer(
    org_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """
    Admin verifies a BAC transfer against bank statement.
    Body: {"transaction_id": "...", "verified": true, "bank_ref": "TRF-123456"}
    If verified=false, suspends account and flags for DTE invalidation.
    """
    body = await request.json()
    transaction_id = body.get("transaction_id")
    verified = body.get("verified", True)
    bank_ref = body.get("bank_ref", "")
    admin_notes = body.get("notes", "")

    if not transaction_id:
        raise HTTPException(400, "transaction_id requerido")

    # Get the transaction
    tx = supabase.table("credit_transactions").select("*").eq("id", transaction_id).single().execute()
    if not tx.data:
        raise HTTPException(404, "Transaccion no encontrada")

    tx_data = tx.data
    now_str = datetime.utcnow().isoformat()

    if verified:
        # Mark as verified
        supabase.table("credit_transactions").update({
            "verified": True,
            "verified_at": now_str,
            "verified_by": admin["user_id"],
            "bank_ref": bank_ref,
            "admin_notes": admin_notes,
        }).eq("id", transaction_id).execute()

        return {
            "status": "verified",
            "transaction_id": transaction_id,
            "org_id": tx_data["org_id"],
            "amount": tx_data["amount"],
            "message": f"Transferencia verificada. Ref bancaria: {bank_ref}",
        }
    else:
        # FRAUD: Suspend account + reverse credits + flag DTE for invalidation
        org_id_tx = tx_data["org_id"]
        credits_to_reverse = tx_data["amount"]

        # Reverse credits
        org = supabase.table("organizations").select("credit_balance").eq("id", org_id_tx).single().execute()
        if org.data:
            current = org.data["credit_balance"]
            new_balance = max(0, current - credits_to_reverse)
            supabase.table("organizations").update({
                "credit_balance": new_balance,
                "plan_status": "suspended",
            }).eq("id", org_id_tx).execute()

            # Record reversal transaction
            supabase.table("credit_transactions").insert({
                "org_id": org_id_tx,
                "type": "reversal",
                "amount": -credits_to_reverse,
                "balance": new_balance,
                "payment_ref": f"fraud_reversal:{transaction_id}",
                "admin_notes": f"Transferencia no verificada. {admin_notes}",
            }).execute()

        # Mark original transaction as fraudulent
        supabase.table("credit_transactions").update({
            "verified": False,
            "verified_at": now_str,
            "verified_by": admin["user_id"],
            "admin_notes": f"FRAUDE: {admin_notes}",
        }).eq("id", transaction_id).execute()

        # Flag invoice for invalidation if one was emitted
        invoice_codigo = tx_data.get("invoice_codigo")

        return {
            "status": "fraud_detected",
            "transaction_id": transaction_id,
            "org_id": org_id_tx,
            "credits_reversed": credits_to_reverse,
            "account_suspended": True,
            "invoice_to_invalidate": invoice_codigo,
            "message": f"Cuenta suspendida. {credits_to_reverse} creditos revertidos. "
                       f"{'DTE ' + invoice_codigo + ' marcado para invalidacion.' if invoice_codigo else 'Sin DTE asociado.'}",
        }


# ══════════════════════════════════════════════════════════
# ADMIN VIEW/EDIT ANY ORG CONFIG (DTE credentials, certs)
# ══════════════════════════════════════════════════════════

@router.get("/organizations/{org_id}/full-config")
async def admin_get_org_full_config(
    org_id: str,
    admin: dict = Depends(require_admin),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Admin gets full config for any org: org details, credentials, API keys, WhatsApp, credits."""
    org = supabase.table("organizations").select("*").eq("id", org_id).single().execute()
    creds = supabase.table("dte_credentials").select("id,nit,nrc,nombre_emisor,ambiente,created_at").eq("org_id", org_id).execute()
    keys = supabase.table("api_keys").select("id,key_prefix,name,is_active,last_used_at,created_by,created_at").eq("org_id", org_id).execute()
    wa = supabase.table("org_whatsapp_config").select("*").eq("org_id", org_id).execute()
    credits = supabase.table("credit_transactions").select("*").eq("org_id", org_id).order("created_at", desc=True).limit(10).execute()
    receptores = supabase.table("receptores_frecuentes").select("id", count="exact").eq("org_id", org_id).execute()
    dtes_emitidos = supabase.table("dtes").select("id", count="exact").eq("org_id", org_id).execute()
    dtes_recibidos = supabase.table("dte_recibidos").select("id", count="exact").eq("org_id", org_id).execute()

    return {
        "organization": org.data,
        "credentials": creds.data or [],
        "api_keys": keys.data or [],
        "whatsapp": wa.data[0] if wa.data else None,
        "recent_credits": credits.data or [],
        "counts": {
            "receptores": receptores.count or 0,
            "dtes_emitidos": dtes_emitidos.count or 0,
            "dtes_recibidos": dtes_recibidos.count or 0,
        },
    }
