"""
FACTURA-SV: Self-Service Onboarding Router
============================================
Permite a usuarios nuevos (registrados via Supabase Auth, sin org)
crear su primera organización y empezar a usar el sistema.

Usa autenticación JWT directa (no requiere fila en tabla `users`).
"""
import uuid
import secrets
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from supabase import Client as SupabaseClient

from app.dependencies import get_supabase

logger = logging.getLogger(__name__)
security = HTTPBearer()

router = APIRouter(prefix="/api/v1/onboarding", tags=["onboarding"])

WELCOME_CREDITS = 10


# ── JWT-only auth (no users table required) ──

async def get_jwt_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: SupabaseClient = Depends(get_supabase),
) -> dict:
    """
    Validate Supabase JWT and return basic user info.
    Does NOT require a row in the `users` table — this is intentional
    for new self-registered users who haven't completed onboarding.
    """
    from app.dependencies import _get_auth_client

    token = credentials.credentials
    auth_client = _get_auth_client()

    try:
        user_response = auth_client.auth.get_user(token)
        user = user_response.user
        if not user:
            raise HTTPException(401, "Token inválido o expirado")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Token inválido o expirado")

    return {
        "user_id": str(user.id),
        "email": user.email or "",
    }


# ── Schemas ──

class CreateOrgRequest(BaseModel):
    nombre: str = Field(..., min_length=2, max_length=200)
    nit: str = Field("", max_length=20)


# ── Endpoints ──

@router.get("/status")
async def onboarding_status(
    jwt_user: dict = Depends(get_jwt_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """Check if the authenticated user has an organization."""
    user_id = jwt_user["user_id"]

    # Check user_organizations table
    result = db.table("user_organizations").select(
        "org_id"
    ).eq("user_id", user_id).execute()

    has_org = bool(result.data)

    return {
        "has_org": has_org,
        "org_count": len(result.data) if result.data else 0,
    }


@router.post("/create-org")
async def create_first_organization(
    body: CreateOrgRequest,
    jwt_user: dict = Depends(get_jwt_user),
    db: SupabaseClient = Depends(get_supabase),
):
    """
    Create first organization for a self-registered user.

    Flow:
    1. Verify user doesn't already have an org
    2. Create organization with free plan + welcome credits
    3. Create/update `users` table row
    4. Create `user_organizations` membership (admin role)
    5. Log welcome credits transaction
    """
    user_id = jwt_user["user_id"]
    email = jwt_user["email"]
    now = datetime.utcnow().isoformat()

    # 1. Check if user already has an org
    existing = db.table("user_organizations").select(
        "org_id"
    ).eq("user_id", user_id).execute()

    if existing.data:
        return {
            "success": True,
            "org_id": existing.data[0]["org_id"],
            "message": "Ya tiene una organización asignada",
        }

    # 2. Check NIT uniqueness if provided
    nit = body.nit.strip()
    if nit:
        nit_check = db.table("organizations").select("id").eq("nit", nit).execute()
        if nit_check.data:
            raise HTTPException(
                400,
                f"Ya existe una organización con NIT {nit}. "
                "Si es su empresa, pida al administrador que lo vincule.",
            )

    # 3. Create organization
    org_id = str(uuid.uuid4())
    link_code = f"FSV-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"

    org_data = {
        "id": org_id,
        "name": body.nombre.strip(),
        "nit": nit,
        "nrc": "",
        "plan": "free",
        "plan_status": "active",
        "is_active": True,
        "payment_method": "free",
        "monthly_quota": 50,
        "max_companies": 999,
        "credit_balance": WELCOME_CREDITS,
        "link_code": link_code,
        "link_code_enabled": True,
        "created_at": now,
        "updated_at": now,
    }

    try:
        org_result = db.table("organizations").insert(org_data).execute()
        if not org_result.data:
            raise HTTPException(500, "Error creando organización")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Onboarding org insert failed: {e}")
        raise HTTPException(500, "Error creando organización")

    # 4. Create users table row (upsert in case partial state exists)
    try:
        db.table("users").upsert({
            "id": user_id,
            "email": email,
            "full_name": body.nombre.strip(),
            "org_id": org_id,
            "role": "admin",
            "created_at": now,
        }).execute()
    except Exception as e:
        # Rollback org
        try:
            db.table("organizations").delete().eq("id", org_id).execute()
        except Exception:
            pass
        logger.error(f"Onboarding users insert failed: {e}")
        raise HTTPException(500, "Error registrando usuario")

    # 5. Create user_organizations membership
    try:
        db.table("user_organizations").insert({
            "user_id": user_id,
            "org_id": org_id,
            "role": "admin",
            "is_default": True,
        }).execute()
    except Exception as e:
        logger.error(f"Onboarding user_organizations insert failed: {e}")
        # Non-blocking — user row already has org_id

    # 6. Log welcome credits
    try:
        db.table("credit_transactions").insert({
            "org_id": org_id,
            "type": "cortesia",
            "amount": WELCOME_CREDITS,
            "balance": WELCOME_CREDITS,
            "description": "Créditos de bienvenida — registro self-service",
        }).execute()
    except Exception as e:
        logger.error(f"Onboarding credit_transactions insert failed: {e}")
        # Non-blocking

    logger.info(
        f"Self-service onboarding complete: user={email}, org={body.nombre.strip()}, "
        f"org_id={org_id}, credits={WELCOME_CREDITS}"
    )

    return {
        "success": True,
        "org_id": org_id,
        "link_code": link_code,
        "creditos": WELCOME_CREDITS,
        "message": f"Organización '{body.nombre.strip()}' creada con {WELCOME_CREDITS} créditos de bienvenida",
    }
