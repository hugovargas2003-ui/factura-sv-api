"""
FACTURA-SV: Servicio Multi-Sucursal (T2-01)
============================================
CRUD sucursales + resolución de códigos para emisión DTE.
"""
import logging
from uuid import uuid4
from supabase import Client as SupabaseClient

logger = logging.getLogger("factura-sv.sucursal_service")


async def list_sucursales(db: SupabaseClient, org_id: str, solo_activas: bool = True):
    query = db.table("sucursales").select("*").eq("org_id", org_id)
    if solo_activas:
        query = query.eq("activa", True)
    result = query.order("is_default", desc=True).order("nombre").execute()
    return result.data or []


async def get_sucursal(db: SupabaseClient, org_id: str, sucursal_id: str):
    result = db.table("sucursales").select("*").eq("id", sucursal_id).eq("org_id", org_id).execute()
    if not result.data:
        return None
    return result.data[0]


async def create_sucursal(db: SupabaseClient, org_id: str, data: dict):
    record = {
        "id": str(uuid4()),
        "org_id": org_id,
        "nombre": data["nombre"],
        "codigo_establecimiento": data.get("codigo_establecimiento", "S001"),
        "codigo_punto_venta": data.get("codigo_punto_venta", "P001"),
        "tipo_establecimiento": data.get("tipo_establecimiento", "01"),
        "departamento": data.get("departamento"),
        "municipio": data.get("municipio"),
        "direccion_complemento": data.get("direccion_complemento"),
        "telefono": data.get("telefono"),
        "correo": data.get("correo"),
        "is_default": data.get("is_default", False),
    }
    # Si es default, quitar default de las demás
    if record["is_default"]:
        db.table("sucursales").update({"is_default": False}).eq("org_id", org_id).execute()
    result = db.table("sucursales").insert(record).execute()
    return result.data[0] if result.data else record


async def update_sucursal(db: SupabaseClient, org_id: str, sucursal_id: str, data: dict):
    allowed = ["nombre", "codigo_establecimiento", "codigo_punto_venta", "tipo_establecimiento",
               "departamento", "municipio", "direccion_complemento", "telefono", "correo", "is_default", "activa"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return None
    if updates.get("is_default"):
        db.table("sucursales").update({"is_default": False}).eq("org_id", org_id).execute()
    result = db.table("sucursales").update(updates).eq("id", sucursal_id).eq("org_id", org_id).execute()
    return result.data[0] if result.data else None


async def delete_sucursal(db: SupabaseClient, org_id: str, sucursal_id: str):
    # No permitir borrar la default
    suc = await get_sucursal(db, org_id, sucursal_id)
    if not suc:
        return {"error": "Sucursal no encontrada"}
    if suc.get("is_default"):
        return {"error": "No se puede eliminar la sucursal por defecto"}
    db.table("sucursales").update({"activa": False}).eq("id", sucursal_id).eq("org_id", org_id).execute()
    return {"status": "deactivated", "id": sucursal_id}


async def resolve_sucursal_codes(db: SupabaseClient, org_id: str, sucursal_id: str = None):
    """Resuelve códigos de establecimiento/punto venta para emisión DTE.
    Si viene sucursal_id, usa esa. Si no, usa la default de la org."""
    if sucursal_id:
        suc = await get_sucursal(db, org_id, sucursal_id)
        if suc:
            return {
                "codigo_establecimiento": suc["codigo_establecimiento"],
                "codigo_punto_venta": suc["codigo_punto_venta"],
                "tipo_establecimiento": suc["tipo_establecimiento"],
                "departamento": suc.get("departamento"),
                "municipio": suc.get("municipio"),
                "direccion_complemento": suc.get("direccion_complemento"),
                "sucursal_id": suc["id"],
                "sucursal_nombre": suc["nombre"],
            }
    # Default
    result = db.table("sucursales").select("*").eq("org_id", org_id).eq("is_default", True).limit(1).execute()
    if result.data:
        suc = result.data[0]
        return {
            "codigo_establecimiento": suc["codigo_establecimiento"],
            "codigo_punto_venta": suc["codigo_punto_venta"],
            "tipo_establecimiento": suc["tipo_establecimiento"],
            "departamento": suc.get("departamento"),
            "municipio": suc.get("municipio"),
            "direccion_complemento": suc.get("direccion_complemento"),
            "sucursal_id": suc["id"],
            "sucursal_nombre": suc["nombre"],
        }
    return None
