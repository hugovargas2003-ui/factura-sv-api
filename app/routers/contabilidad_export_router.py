"""
Contabilidad Export Router — XLSX profesional para Libro Diario y Estado de Resultados.
NO modifica endpoints existentes de contabilidad (esos están en dte_router.py).
"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Query, Response

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/contabilidad", tags=["contabilidad-export"])

from app.dependencies import get_current_user, get_supabase
from app.services.contabilidad_service import list_journal_entries, get_balance_general
from app.services.contabilidad_export_service import generate_libro_diario_xlsx, generate_estado_resultados_xlsx


def _get_org_info(supabase, org_id: str) -> dict:
    """Get org name and NIT for report headers."""
    try:
        org = supabase.table("organizations").select("name, nit").eq("id", org_id).execute()
        if org.data:
            return {"name": org.data[0].get("name", ""), "nit": org.data[0].get("nit", "")}
    except Exception:
        pass
    return {"name": "", "nit": ""}


def _parse_periodo(periodo: str) -> tuple[str | None, str | None]:
    """Parse MMYYYY or YYYY into (fecha_from, fecha_to)."""
    if len(periodo) == 6:  # MMYYYY
        mes = int(periodo[:2])
        anio = int(periodo[2:])
        fecha_from = f"{anio}-{mes:02d}-01"
        if mes == 12:
            fecha_to = f"{anio + 1}-01-01"
        else:
            fecha_to = f"{anio}-{mes + 1:02d}-01"
        return fecha_from, fecha_to
    elif len(periodo) == 4:  # YYYY
        return f"{periodo}-01-01", f"{int(periodo) + 1}-01-01"
    return None, None


@router.get("/libro-diario/export")
async def export_libro_diario(
    periodo: str = Query(None, description="MMYYYY o YYYY — filtra por mes o año"),
    fecha_from: str = Query(None, description="Fecha inicio YYYY-MM-DD"),
    fecha_to: str = Query(None, description="Fecha fin YYYY-MM-DD"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Exportar Libro Diario en XLSX con formato contable profesional."""
    org_id = user.get("org_id")

    # Parse periodo to date range if provided
    if periodo and not fecha_from:
        fecha_from, fecha_to = _parse_periodo(periodo)

    # Get entries using existing service function
    result = await list_journal_entries(supabase, org_id, fecha_from=fecha_from, fecha_to=fecha_to, per_page=9999)
    entries = result.get("data", [])

    if not entries:
        raise HTTPException(status_code=404, detail="No hay partidas para el período seleccionado")

    org_info = _get_org_info(supabase, org_id)
    periodo_label = periodo or (f"{fecha_from} a {fecha_to}" if fecha_from else "Todos")

    xlsx_bytes = generate_libro_diario_xlsx(entries, org_info["name"], org_info["nit"], periodo_label)

    filename = f"Libro_Diario_{periodo or 'completo'}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/estado-resultados/export")
async def export_estado_resultados(
    periodo: str = Query(None, description="MMYYYY o YYYY"),
    fecha_corte: str = Query(None, description="Fecha de corte YYYY-MM-DD"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Exportar Estado de Resultados en XLSX profesional."""
    org_id = user.get("org_id")

    # Parse periodo to fecha_corte (use end of period)
    if periodo and not fecha_corte:
        _, fecha_corte = _parse_periodo(periodo)

    # Get balance using existing service function
    result = await get_balance_general(supabase, org_id, fecha_corte=fecha_corte)
    cuentas = result.get("cuentas", [])

    if not cuentas:
        raise HTTPException(status_code=404, detail="No hay datos contables para el período")

    org_info = _get_org_info(supabase, org_id)
    periodo_label = periodo or (f"Al {fecha_corte}" if fecha_corte else "Acumulado")

    xlsx_bytes = generate_estado_resultados_xlsx(cuentas, org_info["name"], org_info["nit"], periodo_label)

    filename = f"Estado_Resultados_{periodo or 'completo'}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
