"""
FACTURA-SV: Dashboard Avanzado (T2-03)
=======================================
Tendencias de ventas, top clientes, top productos.
"""
import logging
from datetime import date, timedelta
from supabase import Client as SupabaseClient

logger = logging.getLogger("factura-sv.dashboard_advanced")


async def get_ventas_diarias(db: SupabaseClient, org_id: str, dias: int = 30):
    """Ventas agrupadas por día, últimos N días."""
    desde = (date.today() - timedelta(days=dias)).isoformat()
    result = db.table("dtes").select(
        "fecha_emision, monto_total, tipo_dte"
    ).eq("org_id", org_id).eq("estado", "PROCESADO").gte(
        "fecha_emision", desde
    ).order("fecha_emision").execute()

    daily = {}
    for r in (result.data or []):
        fecha = r["fecha_emision"][:10]
        if fecha not in daily:
            daily[fecha] = {"fecha": fecha, "total": 0, "cantidad": 0, "facturas": 0, "ccf": 0}
        daily[fecha]["total"] += float(r.get("monto_total") or 0)
        daily[fecha]["cantidad"] += 1
        if r.get("tipo_dte") == "01":
            daily[fecha]["facturas"] += 1
        elif r.get("tipo_dte") == "03":
            daily[fecha]["ccf"] += 1

    # Fill gaps
    result_list = []
    current = date.today() - timedelta(days=dias)
    while current <= date.today():
        ds = current.isoformat()
        if ds in daily:
            result_list.append(daily[ds])
        else:
            result_list.append({"fecha": ds, "total": 0, "cantidad": 0, "facturas": 0, "ccf": 0})
        current += timedelta(days=1)
    return result_list


async def get_top_clientes(db: SupabaseClient, org_id: str, limit: int = 10):
    """Top clientes por monto total facturado."""
    result = db.table("dtes").select(
        "receptor_nombre, receptor_nit, monto_total"
    ).eq("org_id", org_id).eq("estado", "PROCESADO").execute()

    clientes = {}
    for r in (result.data or []):
        nombre = r.get("receptor_nombre") or "Sin nombre"
        if nombre not in clientes:
            clientes[nombre] = {"nombre": nombre, "nit": r.get("receptor_nit", ""), "total": 0, "cantidad": 0}
        clientes[nombre]["total"] += float(r.get("monto_total") or 0)
        clientes[nombre]["cantidad"] += 1

    ranked = sorted(clientes.values(), key=lambda x: x["total"], reverse=True)[:limit]
    return ranked


async def get_top_productos(db: SupabaseClient, org_id: str, limit: int = 10):
    """Top productos por cantidad vendida y monto."""
    result = db.table("dte_productos").select(
        "descripcion, codigo, cantidad_total, monto_total"
    ).eq("org_id", org_id).order("monto_total", desc=True).limit(limit).execute()
    return result.data or []


async def get_dashboard_advanced(db: SupabaseClient, org_id: str, dias: int = 30):
    """Endpoint consolidado dashboard avanzado."""
    ventas = await get_ventas_diarias(db, org_id, dias)
    clientes = await get_top_clientes(db, org_id)
    productos = await get_top_productos(db, org_id)

    total_periodo = sum(d["total"] for d in ventas)
    total_docs = sum(d["cantidad"] for d in ventas)
    promedio_diario = total_periodo / max(len([d for d in ventas if d["total"] > 0]), 1)

    return {
        "ventas_diarias": ventas,
        "top_clientes": clientes,
        "top_productos": productos,
        "resumen": {
            "total_periodo": round(total_periodo, 2),
            "total_documentos": total_docs,
            "promedio_diario": round(promedio_diario, 2),
            "dias_con_ventas": len([d for d in ventas if d["total"] > 0]),
        }
    }
