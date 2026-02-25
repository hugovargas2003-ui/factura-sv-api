"""
ProtectTrack Email Service
Sends transactional emails via Google Apps Script
"""
import httpx
import os
import json
from typing import Optional

EMAIL_SCRIPT_URL = os.getenv("EMAIL_SCRIPT_URL", "")

async def send_email(action: str, email: str, lang: str = "es", **kwargs):
    """Send email via Google Apps Script"""
    if not EMAIL_SCRIPT_URL:
        print(f"[Email] ⚠️ EMAIL_SCRIPT_URL not configured. Skipping {action} to {email}")
        return {"success": False, "error": "EMAIL_SCRIPT_URL not configured"}
    
    payload = {
        "action": action,
        "email": email,
        "lang": lang,
        **kwargs
    }
    
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
            response = await client.post(
                EMAIL_SCRIPT_URL,
                content=json.dumps(payload),
                headers={"Content-Type": "text/plain"}
            )
            
            try:
                result = response.json()
            except:
                result = {"success": True, "note": "response not JSON but request sent"}
            
            print(f"[Email] ✅ {action} → {email} ({lang})")
            return result
            
    except Exception as e:
        print(f"[Email] ❌ Failed {action} → {email}: {e}")
        return {"success": False, "error": str(e)}


# ==================== CONVENIENCE FUNCTIONS ====================

async def send_welcome(email: str, name: str, lang: str = "es"):
    return await send_email("welcome", email, lang, name=name)

async def send_password_reset(email: str, name: str, reset_url: str, lang: str = "es"):
    return await send_email("password_reset", email, lang, name=name, reset_url=reset_url)

async def send_subscription_activated(email: str, name: str, plan: str, devices: int, lang: str = "es"):
    return await send_email("subscription_activated", email, lang, name=name, plan=plan, devices=devices)

async def send_subscription_cancelled(email: str, name: str, lang: str = "es"):
    return await send_email("subscription_cancelled", email, lang, name=name)

async def send_sos_alert(email: str, name: str, device_name: str, latitude: float, longitude: float, lang: str = "es"):
    return await send_email("sos_alert", email, lang, name=name, device_name=device_name, latitude=latitude, longitude=longitude)

async def send_device_alert(email: str, device_name: str, alert_type: str, message: str, lang: str = "es"):
    return await send_email("device_alert", email, lang, device_name=device_name, alert_type=alert_type, message=message)
