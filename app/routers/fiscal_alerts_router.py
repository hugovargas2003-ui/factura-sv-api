"""
Fiscal Alerts Router — Endpoint para check de alertas fiscales + comparativo.
"""
import logging
from fastapi import APIRouter, Depends, Query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["fiscal-alerts"])

from app.dependencies import get_current_user, get_supabase
from app.services.fiscal_alerts_service import check_fiscal_alerts


@router.get("/fiscal-alerts/check")
async def check_alerts(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """
    Check fiscal obligations and generate alerts.
    Call on dashboard load or manually. Idempotent — won't duplicate alerts.
    """
    org_id = user.get("org_id")
    user_id = user.get("user_id")
    result = await check_fiscal_alerts(supabase, org_id, user_id)
    return result


@router.get("/analytics/comparativo")
async def comparativo_periodos(
    periodo1: str = Query(..., description="MMYYYY primer período"),
    periodo2: str = Query(..., description="MMYYYY segundo período"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """
    Comparativo fiscal entre dos períodos.
    Retorna diferencias en ventas, compras, IVA, ISR, empleados.
    """
    org_id = user.get("org_id")

    def _get_periodo_data(periodo: str) -> dict:
        mes = int(periodo[:2])
        anio = int(periodo[2:])
        fecha_desde = f"{anio}-{mes:02d}-01"
        if mes == 12:
            fecha_hasta = f"{anio + 1}-01-01"
        else:
            fecha_hasta = f"{anio}-{mes + 1:02d}-01"

        # Ventas (DTEs emitidos)
        ventas = supabase.table("dtes") \
            .select("monto_total, tipo_dte") \
            .eq("org_id", org_id) \
            .in_("estado", ["procesado", "IMPORTADO"]) \
            .gte("fecha_emision", fecha_desde) \
            .lt("fecha_emision", fecha_hasta) \
            .execute()

        total_ventas = sum(float(v.get("monto_total", 0)) for v in (ventas.data or []))
        count_dtes = len(ventas.data or [])

        # Compras (DTEs recibidos)
        compras = supabase.table("dte_recibidos") \
            .select("monto_total, iva_credito") \
            .eq("org_id", org_id) \
            .eq("status", "active") \
            .gte("fec_emi", fecha_desde) \
            .lt("fec_emi", fecha_hasta) \
            .execute()

        total_compras = sum(float(c.get("monto_total", 0)) for c in (compras.data or []))
        total_iva_credito = sum(float(c.get("iva_credito", 0)) for c in (compras.data or []))

        # F-14 (retenciones ISR)
        renta = supabase.table("renta_retenciones") \
            .select("impuesto_retenido") \
            .eq("org_id", org_id) \
            .eq("periodo", periodo) \
            .execute()

        total_isr = sum(float(r.get("impuesto_retenido", 0)) for r in (renta.data or []))

        # Planilla
        planilla = supabase.table("planilla_resumen") \
            .select("total_empleados, total_salarios, total_isr, total_neto") \
            .eq("org_id", org_id) \
            .eq("periodo", periodo) \
            .execute()

        planilla_data = planilla.data[0] if planilla.data else {}

        return {
            "periodo": periodo,
            "ventas_total": round(total_ventas, 2),
            "ventas_dtes": count_dtes,
            "compras_total": round(total_compras, 2),
            "compras_dtes": len(compras.data or []),
            "iva_credito": round(total_iva_credito, 2),
            "isr_retenciones": round(total_isr, 2),
            "empleados": int(planilla_data.get("total_empleados", 0)),
            "salarios": float(planilla_data.get("total_salarios", 0)),
            "planilla_isr": float(planilla_data.get("total_isr", 0)),
            "planilla_neto": float(planilla_data.get("total_neto", 0)),
        }

    p1 = _get_periodo_data(periodo1)
    p2 = _get_periodo_data(periodo2)

    def _pct_change(old: float, new: float) -> float:
        if old == 0:
            return 100.0 if new > 0 else 0.0
        return round((new - old) / old * 100, 1)

    comparativo = {
        "ventas": {
            "p1": p1["ventas_total"], "p2": p2["ventas_total"],
            "diff": round(p2["ventas_total"] - p1["ventas_total"], 2),
            "pct": _pct_change(p1["ventas_total"], p2["ventas_total"]),
            "trend": "up" if p2["ventas_total"] > p1["ventas_total"] else "down" if p2["ventas_total"] < p1["ventas_total"] else "flat",
        },
        "compras": {
            "p1": p1["compras_total"], "p2": p2["compras_total"],
            "diff": round(p2["compras_total"] - p1["compras_total"], 2),
            "pct": _pct_change(p1["compras_total"], p2["compras_total"]),
            "trend": "up" if p2["compras_total"] > p1["compras_total"] else "down" if p2["compras_total"] < p1["compras_total"] else "flat",
        },
        "isr": {
            "p1": p1["isr_retenciones"], "p2": p2["isr_retenciones"],
            "diff": round(p2["isr_retenciones"] - p1["isr_retenciones"], 2),
            "pct": _pct_change(p1["isr_retenciones"], p2["isr_retenciones"]),
        },
        "empleados": {
            "p1": p1["empleados"], "p2": p2["empleados"],
            "diff": p2["empleados"] - p1["empleados"],
        },
        "dtes_emitidos": {
            "p1": p1["ventas_dtes"], "p2": p2["ventas_dtes"],
            "diff": p2["ventas_dtes"] - p1["ventas_dtes"],
        },
    }

    return {
        "periodo1": p1,
        "periodo2": p2,
        "comparativo": comparativo,
    }
