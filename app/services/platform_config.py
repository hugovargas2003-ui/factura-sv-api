"""
FACTURA-SV: Platform Config Service
=====================================
Centralized config reader/writer for platform_config table.
"""

import logging
from typing import Any
from datetime import datetime

logger = logging.getLogger("platform_config")


def _strip_json(val: Any) -> Any:
    """JSONB stores strings with quotes — unwrap if needed."""
    return val


async def get_config(db, key: str, default: Any = None) -> Any:
    """Get a single config value."""
    try:
        result = db.table("platform_config").select("value").eq("key", key).single().execute()
        if result.data:
            return _strip_json(result.data["value"])
    except Exception:
        pass
    return default


async def get_config_category(db, category: str) -> dict[str, Any]:
    """Get all config values for a category as a flat dict."""
    try:
        result = db.table("platform_config").select(
            "key, value, label, description, is_secret"
        ).eq("category", category).order("key").execute()

        out = {}
        for row in (result.data or []):
            short_key = row["key"].split(".", 1)[-1] if "." in row["key"] else row["key"]
            out[short_key] = _strip_json(row["value"])
        return out
    except Exception as e:
        logger.error(f"Error loading config category {category}: {e}")
        return {}


async def get_all_config(db) -> list[dict]:
    """Get all config entries (for admin panel)."""
    try:
        result = db.table("platform_config").select(
            "key, value, category, label, description, is_secret, updated_at, updated_by"
        ).order("category").order("key").execute()
        return result.data or []
    except Exception as e:
        logger.error(f"Error loading all config: {e}")
        return []


async def set_config(db, key: str, value: Any, updated_by: str = None) -> bool:
    """Set a single config value."""
    try:
        result = db.table("platform_config").update({
            "value": value,
            "updated_at": datetime.utcnow().isoformat(),
            "updated_by": updated_by,
        }).eq("key", key).execute()
        return bool(result.data)
    except Exception as e:
        logger.error(f"Error setting config {key}: {e}")
        return False


async def set_config_bulk(db, updates: dict[str, Any], updated_by: str = None) -> int:
    """Set multiple config values at once."""
    count = 0
    for key, value in updates.items():
        if await set_config(db, key, value, updated_by):
            count += 1
    return count


async def get_billing_emisor_from_config(db) -> dict:
    """Build emisor dict from platform_config. Falls back to hardcoded."""
    cfg = await get_config_category(db, "emisor")
    if not cfg:
        return {
            "nit": "06141212711033",
            "nrc": "1549809",
            "nombre": "HUGO ERNESTO VARGAS OLIVA",
            "cod_actividad": "58200",
            "desc_actividad": "Edicion de programas informaticos",
            "nombre_comercial": "EFFICIENT AI ALGORITHMS",
            "tipo_establecimiento": "01",
            "direccion_departamento": "06",
            "direccion_municipio": "14",
            "direccion_complemento": "San Salvador, El Salvador",
            "telefono": "00000000",
            "correo": "hugovargas2003@gmail.com",
            "codigo_establecimiento": "M001",
            "codigo_punto_venta": "P001",
        }
    return cfg


async def get_bank_info_from_config(db) -> dict:
    """Build bank info dict from platform_config."""
    cfg = await get_config_category(db, "banco")
    if not cfg:
        return {
            "titular": "HUGO ERNESTO VARGAS OLIVA",
            "cuenta": "201436482",
            "banco": "Banco América Central (BAC) El Salvador",
            "moneda": "USD",
            "instrucciones": "",
        }
    return cfg


async def get_dte_config(db) -> dict:
    """Get DTE emission config."""
    cfg = await get_config_category(db, "dte")
    if not cfg:
        return {
            "descripcion_template": "Servicio de Facturación Electrónica DTE — Plan {plan_name}",
            "observaciones_stripe": "Pago procesado por Stripe.",
            "observaciones_transfer": "Pago por transferencia bancaria.",
            "forma_pago_default": 5,
            "condicion_operacion": 1,
        }
    return cfg
