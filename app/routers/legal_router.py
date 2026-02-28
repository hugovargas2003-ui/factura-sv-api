"""
FACTURA-SV: Legal Acceptance Endpoints
Aceptación electrónica con audit trail completo
Art. 6 Ley de Firma Electrónica · Art. 14 Ley de Comercio Electrónico
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import hashlib, json

from app.dependencies import get_supabase, get_current_user

router = APIRouter(prefix="/api/v1/legal", tags=["legal"])

# ── Models ──

class AcceptanceRequest(BaseModel):
    document_id: str = Field(..., description="tos, privacy, api, partner, dpa")
    document_version: str = "2.0"
    document_title: str = ""
    organization_id: Optional[str] = None
    signer_name: Optional[str] = None
    signer_nit: Optional[str] = None
    signer_nrc: Optional[str] = None
    signer_cargo: Optional[str] = None
    signer_email: Optional[str] = None
    signer_empresa: Optional[str] = None
    screen_resolution: Optional[str] = None
    browser_language: Optional[str] = None

# ── Helpers ──

def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def _verification_hash(data: dict) -> str:
    canonical = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]

# ── Endpoints ──

@router.post("/accept")
async def accept_document(
    body: AcceptanceRequest,
    request: Request,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Record acceptance of a legal document with full audit trail."""
    now = datetime.utcnow().isoformat()
    ip = _client_ip(request)
    ua = request.headers.get("user-agent", "unknown")

    audit_data = {
        "user_id": user["id"],
        "document_id": body.document_id,
        "document_version": body.document_version,
        "accepted_at": now,
        "ip_address": ip,
        "signer_name": body.signer_name,
        "signer_nit": body.signer_nit,
    }
    vhash = _verification_hash(audit_data)

    raw_audit = {
        **audit_data,
        "user_agent": ua,
        "signer_nrc": body.signer_nrc,
        "signer_cargo": body.signer_cargo,
        "signer_email": body.signer_email,
        "signer_empresa": body.signer_empresa,
        "screen_resolution": body.screen_resolution,
        "browser_language": body.browser_language,
        "verification_hash": vhash,
    }

    # Mark old acceptances for this doc as not current
    supabase.table("legal_acceptances") \
        .update({"is_current": False}) \
        .eq("user_id", user["id"]) \
        .eq("document_id", body.document_id) \
        .eq("is_current", True) \
        .execute()

    result = supabase.table("legal_acceptances").insert({
        "user_id": user["id"],
        "organization_id": body.organization_id,
        "document_id": body.document_id,
        "document_version": body.document_version,
        "document_title": body.document_title or body.document_id.upper(),
        "signer_name": body.signer_name,
        "signer_nit": body.signer_nit,
        "signer_nrc": body.signer_nrc,
        "signer_cargo": body.signer_cargo,
        "signer_email": body.signer_email,
        "signer_empresa": body.signer_empresa,
        "accepted_at": now,
        "ip_address": ip,
        "user_agent": ua,
        "screen_resolution": body.screen_resolution,
        "browser_language": body.browser_language,
        "verification_hash": vhash,
        "raw_audit_json": raw_audit,
        "accepted_via": "web",
        "is_current": True,
    }).execute()

    if not result.data:
        raise HTTPException(500, "Error al registrar aceptación")

    return {
        "id": result.data[0]["id"],
        "document_id": body.document_id,
        "document_version": body.document_version,
        "accepted_at": now,
        "verification_hash": vhash,
        "message": "Documento aceptado exitosamente. Registro de auditoría creado."
    }


@router.get("/status")
async def get_acceptance_status(
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Get which legal documents the user has accepted."""
    result = supabase.table("legal_acceptances") \
        .select("document_id, document_version, document_title, accepted_at, verification_hash, signer_name") \
        .eq("user_id", user["id"]) \
        .eq("is_current", True) \
        .execute()

    accepted = {}
    for row in (result.data or []):
        accepted[row["document_id"]] = {
            "version": row["document_version"],
            "title": row["document_title"],
            "accepted_at": row["accepted_at"],
            "hash": row["verification_hash"],
            "signer": row["signer_name"],
        }

    required = {
        "tos": {"title": "Términos de Servicio", "version": "2.0", "required": True},
        "privacy": {"title": "Política de Privacidad", "version": "2.0", "required": True},
    }
    optional = {
        "api": {"title": "Contrato de Integración API", "version": "2.0", "required": False},
        "partner": {"title": "Contrato de Asociación Comercial", "version": "2.0", "required": False},
        "dpa": {"title": "Acuerdo de Procesamiento de Datos", "version": "1.0", "required": False},
    }

    all_ok = all(
        did in accepted and accepted[did]["version"] == info["version"]
        for did, info in required.items()
    )

    return {
        "accepted": accepted,
        "required_documents": required,
        "optional_documents": optional,
        "all_required_accepted": all_ok,
    }


@router.get("/audit/{document_id}")
async def get_audit_trail(
    document_id: str,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Get full audit history for a document."""
    result = supabase.table("legal_acceptances") \
        .select("*") \
        .eq("user_id", user["id"]) \
        .eq("document_id", document_id) \
        .order("accepted_at", desc=True) \
        .limit(10) \
        .execute()
    return {"acceptances": result.data or []}


@router.get("/verify/{verification_hash}")
async def verify_acceptance(
    verification_hash: str,
    supabase=Depends(get_supabase),
):
    """Public: verify a document acceptance by hash (for auditors/MH)."""
    result = supabase.table("legal_acceptances") \
        .select("document_id, document_version, document_title, accepted_at, signer_name, signer_nit, signer_empresa, verification_hash") \
        .eq("verification_hash", verification_hash) \
        .limit(1) \
        .execute()

    if not result.data:
        raise HTTPException(404, "No se encontró registro con ese hash de verificación")

    row = result.data[0]
    return {
        "verified": True,
        "document": row["document_title"],
        "version": row["document_version"],
        "accepted_at": row["accepted_at"],
        "signer": row["signer_name"],
        "nit": row["signer_nit"],
        "empresa": row["signer_empresa"],
        "hash": row["verification_hash"],
    }


# ── Admin endpoints ──

@router.get("/admin/acceptances")
async def admin_list_acceptances(
    document_id: Optional[str] = None,
    limit: int = 100,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Admin: list all legal acceptances for audit purposes."""
    if user.get("role") != "superadmin":
        raise HTTPException(403, "Solo administradores")

    query = supabase.table("legal_acceptances") \
        .select("*") \
        .eq("is_current", True) \
        .order("accepted_at", desc=True) \
        .limit(limit)

    if document_id:
        query = query.eq("document_id", document_id)

    result = query.execute()

    # Summary stats
    docs = {}
    for row in (result.data or []):
        did = row["document_id"]
        docs[did] = docs.get(did, 0) + 1

    return {
        "acceptances": result.data or [],
        "total": len(result.data or []),
        "by_document": docs,
    }


@router.get("/admin/user/{user_id}")
async def admin_user_acceptances(
    user_id: str,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Admin: get all acceptances for a specific user."""
    if user.get("role") != "superadmin":
        raise HTTPException(403, "Solo administradores")

    result = supabase.table("legal_acceptances") \
        .select("*") \
        .eq("user_id", user_id) \
        .order("accepted_at", desc=True) \
        .execute()

    return {"user_id": user_id, "acceptances": result.data or [], "total": len(result.data or [])}


@router.get("/admin/org/{org_id}")
async def admin_org_acceptances(
    org_id: str,
    user=Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Admin: get all acceptances linked to an organization."""
    if user.get("role") != "superadmin":
        raise HTTPException(403, "Solo administradores")

    result = supabase.table("legal_acceptances") \
        .select("*") \
        .eq("organization_id", org_id) \
        .order("accepted_at", desc=True) \
        .execute()

    return {"organization_id": org_id, "acceptances": result.data or [], "total": len(result.data or [])}
