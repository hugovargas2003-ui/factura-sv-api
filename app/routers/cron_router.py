"""
cron_router.py — Scheduled task endpoints for FACTURA-SV.

Location: app/routers/cron_router.py

Endpoints:
  GET /api/v1/cron/check-expirations?key=<SECRET> — Send expiry notifications
  
Security: Protected by CRON_SECRET query param (not JWT).
Designed to be called by Railway cron, external scheduler, or manually.
"""

import os
import logging
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger("cron")

router = APIRouter(prefix="/cron", tags=["Cron Jobs"])

CRON_SECRET = os.getenv("CRON_SECRET", "fsv-cron-2026")


@router.get("/check-expirations")
async def check_expirations(key: str = Query(..., description="Cron secret key")):
    """
    Scan all organizations and send expiry notification emails.
    
    Call daily via cron or manually:
      GET /api/v1/cron/check-expirations?key=fsv-cron-2026
    """
    if key != CRON_SECRET:
        raise HTTPException(403, "Invalid cron key")

    from app.dependencies import get_supabase
    from app.services.subscription_notifier import check_and_notify

    supabase = get_supabase()
    results = await check_and_notify(supabase)

    logger.info(f"Cron check-expirations: {results['sent']} sent, {results['errors']} errors")

    return {
        "success": True,
        "summary": {
            "organizations_checked": results["checked"],
            "emails_sent": results["sent"],
            "skipped": results["skipped"],
            "errors": results["errors"],
        },
        "details": results["details"],
    }


@router.get("/health")
async def cron_health():
    """Health check for cron system."""
    return {"status": "ok", "service": "cron"}
