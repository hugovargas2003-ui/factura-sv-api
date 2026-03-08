"""
Contador Batch Router — Genera F-07 y F-14 para todas las empresas del despacho.
Endpoints batch que iteran por las orgs vinculadas al contador.
"""
import logging
import io
import zipfile
import csv as csv_mod
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/contador", tags=["contador-batch"])

from app.dependencies import get_current_user, get_supabase


def _get_contador_orgs(supabase, user_id: str) -> list:
    """
    Get all organizations accessible to this user (for batch operations).
    Returns list of {id, name, nit, nrc} for each org.
    """
    memberships = supabase.table("user_organizations") \
        .select("org_id") \
        .eq("user_id", user_id) \
        .execute()

    if not memberships.data:
        return []

    org_ids = [m["org_id"] for m in memberships.data]

    orgs = supabase.table("organizations") \
        .select("id, name, nit, nrc") \
        .in_("id", org_ids) \
        .execute()

    return orgs.data or []


def _generate_f07_csv_for_org(supabase, org_id: str, periodo: str) -> Optional[dict]:
    """
    Generate F-07 CSV content for a single org.
    Returns {anexo1: str, anexo2: str, anexo3: str, count: int} or None if no data.
    """
    if len(periodo) != 6:
        return None

    mes = int(periodo[:2])
    anio = int(periodo[2:])
    fecha_desde = f"{anio}-{mes:02d}-01"
    if mes == 12:
        fecha_hasta = f"{anio + 1}-01-01"
    else:
        fecha_hasta = f"{anio}-{mes + 1:02d}-01"

    dtes = supabase.table("dtes") \
        .select("tipo_dte, fecha_emision, numero_control, codigo_generacion, "
                "receptor_nit, receptor_nombre, monto_total, json_data") \
        .eq("org_id", org_id) \
        .in_("estado", ["procesado", "IMPORTADO"]) \
        .gte("fecha_emision", fecha_desde) \
        .lt("fecha_emision", fecha_hasta) \
        .execute()

    if not dtes.data:
        return None

    anexo1_rows = []  # CCF (03) + NC (05) + ND (06)
    anexo2_rows = []  # Facturas (01) + Sujeto Excluido (14)
    anexo3_rows = []  # Retenciones (07)

    for dte in dtes.data:
        tipo = dte.get("tipo_dte", "")
        json_data = dte.get("json_data") or {}
        if isinstance(json_data, str):
            import json
            try:
                json_data = json.loads(json_data)
            except Exception:
                json_data = {}

        resumen = json_data.get("resumen", {}) if isinstance(json_data, dict) else {}

        row_base = {
            "fecha": dte.get("fecha_emision", ""),
            "tipo": tipo,
            "numero_control": dte.get("numero_control", ""),
            "codigo_generacion": dte.get("codigo_generacion", ""),
            "receptor_nit": dte.get("receptor_nit", ""),
            "receptor_nombre": dte.get("receptor_nombre", ""),
            "gravada": resumen.get("totalGravada", dte.get("monto_total", 0)),
            "exenta": resumen.get("totalExenta", 0),
            "no_sujeta": resumen.get("totalNoSuj", 0),
            "iva": resumen.get("montoTotalOperacion", 0),
            "total": resumen.get("totalPagar", dte.get("monto_total", 0)),
        }

        if tipo in ("03", "05", "06"):
            anexo1_rows.append(row_base)
        elif tipo in ("01", "14"):
            anexo2_rows.append(row_base)
        elif tipo == "07":
            anexo3_rows.append(row_base)

    def rows_to_csv(rows):
        if not rows:
            return ""
        output = io.StringIO()
        writer = csv_mod.writer(output, delimiter=';')
        for r in rows:
            writer.writerow([
                r["fecha"], r["tipo"], r["numero_control"], r["codigo_generacion"],
                r["receptor_nit"], r["receptor_nombre"],
                r["gravada"], r["exenta"], r["no_sujeta"], r["iva"], r["total"],
            ])
        return output.getvalue()

    return {
        "anexo1": rows_to_csv(anexo1_rows),
        "anexo2": rows_to_csv(anexo2_rows),
        "anexo3": rows_to_csv(anexo3_rows),
        "count": len(dtes.data),
    }


def _generate_f14_csv_for_org(supabase, org_id: str, periodo: str) -> Optional[str]:
    """Generate F-14 CSV for a single org. Returns CSV string or None."""
    retenciones = supabase.table("renta_retenciones") \
        .select("*") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .order("created_at") \
        .execute()

    if not retenciones.data:
        return None

    output = io.StringIO()
    writer = csv_mod.writer(output, delimiter=';', quoting=csv_mod.QUOTE_MINIMAL)

    for r in retenciones.data:
        writer.writerow([
            r.get("domicilio", 1),
            r.get("codigo_pais", "9300"),
            r.get("nombre_razon", ""),
            r.get("nit_nif", ""),
            r.get("dui", ""),
            r.get("codigo_ingreso", ""),
            r.get("monto_devengado", 0),
            r.get("monto_bonificaciones", 0),
            r.get("impuesto_retenido", 0),
            r.get("aguinaldo_exento", 0),
            r.get("aguinaldo_gravado", 0),
            r.get("afp", 0),
            r.get("isss", 0),
            r.get("inpep", 0),
            r.get("ipsfa", 0),
            r.get("cefafa", 0),
            r.get("bienestar_mag", 0),
            r.get("isss_ivm", 0),
            r.get("tipo_operacion", 1),
            r.get("clasificacion", 2),
            r.get("sector", 4),
            r.get("tipo_costo_gasto", 1),
            periodo,
        ])

    return output.getvalue()


# ═══════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════

@router.post("/batch-f07")
async def batch_f07(
    periodo: str = Query(..., description="MMYYYY"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """
    Genera F-07 ZIP para TODAS las empresas del contador.
    Retorna ZIP con subcarpetas por NIT, cada una con Anexo 1+2+3.
    """
    user_id = user.get("user_id")
    orgs = _get_contador_orgs(supabase, user_id)

    if not orgs:
        raise HTTPException(status_code=404, detail="No tiene empresas vinculadas")

    zip_buffer = io.BytesIO()
    orgs_processed = 0
    orgs_empty = 0

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for org in orgs:
            org_id = org["id"]
            org_name = org.get("name", "Sin_nombre")
            org_nit = org.get("nit", "Sin_NIT")
            folder = f"{org_nit}_{org_name[:30]}"

            result = _generate_f07_csv_for_org(supabase, org_id, periodo)

            if result is None:
                orgs_empty += 1
                zf.writestr(f"{folder}/SIN_DATOS_{periodo}.txt",
                    f"No se encontraron DTEs para {org_name} en período {periodo}")
                continue

            if result["anexo1"]:
                zf.writestr(f"{folder}/Anexo_1_F07_{periodo}.csv", result["anexo1"])
            if result["anexo2"]:
                zf.writestr(f"{folder}/Anexo_2_F07_{periodo}.csv", result["anexo2"])
            if result["anexo3"]:
                zf.writestr(f"{folder}/Anexo_3_F07_{periodo}.csv", result["anexo3"])

            orgs_processed += 1

        summary = f"RESUMEN BATCH F-07 — Período {periodo}\n"
        summary += f"Fecha generación: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        summary += f"Empresas procesadas: {orgs_processed}\n"
        summary += f"Empresas sin datos: {orgs_empty}\n"
        summary += f"Total empresas: {len(orgs)}\n\n"
        for org in orgs:
            summary += f"- {org.get('nit', 'N/A')} | {org.get('name', 'N/A')}\n"
        zf.writestr("_RESUMEN.txt", summary)

    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=F07_BATCH_{periodo}_TODAS.zip"},
    )


@router.post("/batch-f14")
async def batch_f14(
    periodo: str = Query(..., description="MMYYYY"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """
    Genera F-14 CSV para TODAS las empresas del contador.
    Retorna ZIP con un CSV por empresa.
    """
    user_id = user.get("user_id")
    orgs = _get_contador_orgs(supabase, user_id)

    if not orgs:
        raise HTTPException(status_code=404, detail="No tiene empresas vinculadas")

    zip_buffer = io.BytesIO()
    orgs_processed = 0
    orgs_empty = 0

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for org in orgs:
            org_id = org["id"]
            org_name = org.get("name", "Sin_nombre")
            org_nit = org.get("nit", "Sin_NIT")

            csv_content = _generate_f14_csv_for_org(supabase, org_id, periodo)

            if csv_content:
                zf.writestr(f"{org_nit}_{org_name[:30]}/F14_ANEXO_{periodo}.csv", csv_content)
                orgs_processed += 1
            else:
                zf.writestr(f"{org_nit}_{org_name[:30]}/SIN_DATOS_{periodo}.txt",
                    f"No se encontraron retenciones F-14 para {org_name} en período {periodo}")
                orgs_empty += 1

        summary = f"RESUMEN BATCH F-14 — Período {periodo}\n"
        summary += f"Fecha generación: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        summary += f"Empresas con F-14: {orgs_processed}\n"
        summary += f"Empresas sin datos: {orgs_empty}\n"
        summary += f"Total empresas: {len(orgs)}\n"
        zf.writestr("_RESUMEN.txt", summary)

    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=F14_BATCH_{periodo}_TODAS.zip"},
    )


@router.get("/resumen-fiscal")
async def resumen_fiscal_multi_org(
    periodo: str = Query(..., description="MMYYYY"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """
    Dashboard ejecutivo: estado fiscal de TODAS las empresas del contador.
    Retorna semáforo por empresa: F-07, F-14, planilla, créditos.
    """
    user_id = user.get("user_id")
    orgs = _get_contador_orgs(supabase, user_id)

    if not orgs:
        return {"empresas": [], "total": 0, "periodo": periodo}

    mes = int(periodo[:2])
    anio = int(periodo[2:])
    fecha_desde = f"{anio}-{mes:02d}-01"
    if mes == 12:
        fecha_hasta = f"{anio + 1}-01-01"
    else:
        fecha_hasta = f"{anio}-{mes + 1:02d}-01"

    empresas = []

    for org in orgs:
        org_id = org["id"]
        org_data = {
            "id": org_id,
            "name": org.get("name", ""),
            "nit": org.get("nit", ""),
        }

        # F-07: Check if DTEs exist for period
        dtes = supabase.table("dtes") \
            .select("id", count="exact") \
            .eq("org_id", org_id) \
            .in_("estado", ["procesado", "IMPORTADO"]) \
            .gte("fecha_emision", fecha_desde) \
            .lt("fecha_emision", fecha_hasta) \
            .execute()
        dte_count = dtes.count if hasattr(dtes, 'count') and dtes.count else len(dtes.data or [])
        org_data["f07_dtes"] = dte_count
        org_data["f07_status"] = "ok" if dte_count > 0 else "empty"

        # F-14: Check renta_periodos status
        renta = supabase.table("renta_periodos") \
            .select("status, total_retenciones, total_registros") \
            .eq("org_id", org_id) \
            .eq("periodo", periodo) \
            .execute()
        if renta.data:
            r = renta.data[0]
            org_data["f14_status"] = r.get("status", "draft")
            org_data["f14_retenciones"] = r.get("total_retenciones", 0)
            org_data["f14_registros"] = r.get("total_registros", 0)
        else:
            org_data["f14_status"] = "missing"
            org_data["f14_retenciones"] = 0
            org_data["f14_registros"] = 0

        # Planilla: Check if confirmed for period
        planilla = supabase.table("planilla_resumen") \
            .select("status, total_empleados, total_isr") \
            .eq("org_id", org_id) \
            .eq("periodo", periodo) \
            .execute()
        if planilla.data:
            p = planilla.data[0]
            org_data["planilla_status"] = p.get("status", "draft")
            org_data["planilla_empleados"] = p.get("total_empleados", 0)
            org_data["planilla_isr"] = p.get("total_isr", 0)
        else:
            org_data["planilla_status"] = "missing"
            org_data["planilla_empleados"] = 0
            org_data["planilla_isr"] = 0

        # Créditos
        credits = supabase.table("organizations") \
            .select("credit_balance") \
            .eq("id", org_id) \
            .execute()
        org_data["creditos"] = credits.data[0].get("credit_balance", 0) if credits.data else 0

        # Overall health
        statuses = [org_data["f07_status"], org_data["f14_status"], org_data["planilla_status"]]
        if "missing" in statuses:
            org_data["health"] = "red"
        elif all(s in ("ok", "confirmed", "exported", "presented") for s in statuses):
            org_data["health"] = "green"
        else:
            org_data["health"] = "yellow"

        empresas.append(org_data)

    # Sort: red first, then yellow, then green
    order = {"red": 0, "yellow": 1, "green": 2}
    empresas.sort(key=lambda e: order.get(e.get("health", "red"), 0))

    return {
        "periodo": periodo,
        "total_empresas": len(empresas),
        "empresas": empresas,
        "resumen": {
            "green": sum(1 for e in empresas if e["health"] == "green"),
            "yellow": sum(1 for e in empresas if e["health"] == "yellow"),
            "red": sum(1 for e in empresas if e["health"] == "red"),
        },
    }
