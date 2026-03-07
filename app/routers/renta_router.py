"""
Renta Router — Declaración F14 (Pago a Cuenta e Impuesto Retenido)
16 endpoints para gestión completa del F14 mensual.
NO modifica ningún archivo existente.
"""
import logging
import csv
import io
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/renta", tags=["renta"])

from app.dependencies import get_current_user, get_supabase


# ═══════════════════════════════════════════
# MODELOS
# ═══════════════════════════════════════════

class PeriodoCreate(BaseModel):
    periodo: str = Field(..., description="Formato MMYYYY, ej: '032026'", min_length=6, max_length=6)
    pago_cuenta_base: Optional[float] = 0

class RetencionCreate(BaseModel):
    periodo: str
    domicilio: int = 1
    codigo_pais: str = "9300"
    nombre_razon: str
    nit_nif: Optional[str] = None
    dui: Optional[str] = None
    codigo_ingreso: str
    monto_devengado: float
    monto_bonificaciones: float = 0
    impuesto_retenido: float = 0
    aguinaldo_exento: float = 0
    aguinaldo_gravado: float = 0
    afp: float = 0
    isss: float = 0
    inpep: float = 0
    ipsfa: float = 0
    cefafa: float = 0
    bienestar_mag: float = 0
    isss_ivm: float = 0
    tipo_operacion: int = 1
    clasificacion: int = 2
    sector: int = 4
    tipo_costo_gasto: int = 1
    origen: str = "manual"

class RetencionUpdate(BaseModel):
    nombre_razon: Optional[str] = None
    nit_nif: Optional[str] = None
    dui: Optional[str] = None
    codigo_ingreso: Optional[str] = None
    monto_devengado: Optional[float] = None
    monto_bonificaciones: Optional[float] = None
    impuesto_retenido: Optional[float] = None
    aguinaldo_exento: Optional[float] = None
    aguinaldo_gravado: Optional[float] = None
    afp: Optional[float] = None
    isss: Optional[float] = None
    tipo_operacion: Optional[int] = None
    clasificacion: Optional[int] = None
    sector: Optional[int] = None
    tipo_costo_gasto: Optional[int] = None

class CalcularISRRequest(BaseModel):
    salario_base: float
    afp: Optional[float] = None
    isss: Optional[float] = None


# ═══════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════

def _calcular_isr_tabla(salario_base: float, afp: float, isss: float, supabase) -> float:
    """Calcula ISR usando tabla progresiva Art. 37 LISR."""
    base_imponible = salario_base - afp - isss
    if base_imponible <= 0:
        return 0.0

    result = supabase.table("renta_tabla_isr") \
        .select("*") \
        .eq("vigencia_desde", "2024-01-01") \
        .order("tramo") \
        .execute()

    if not result.data:
        return 0.0

    for tramo in result.data:
        desde = float(tramo["desde"])
        hasta = float(tramo["hasta"]) if tramo["hasta"] else float('inf')
        if desde <= base_imponible <= hasta:
            tasa = float(tramo["tasa"])
            cuota = float(tramo["cuota_fija"])
            exceso = float(tramo["sobre_exceso"])
            if tasa == 0:
                return 0.0
            return round(cuota + (base_imponible - exceso) * tasa, 2)

    return 0.0


def _update_periodo_totals(supabase, org_id: str, periodo: str):
    """Recalcula totales del período F14."""
    retenciones = supabase.table("renta_retenciones") \
        .select("impuesto_retenido") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()

    total = sum(float(r["impuesto_retenido"]) for r in (retenciones.data or []))
    count = len(retenciones.data or [])

    supabase.table("renta_periodos") \
        .update({
            "total_retenciones": total,
            "total_registros": count,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }) \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()


# ═══════════════════════════════════════════
# ENDPOINTS — PERIODOS
# ═══════════════════════════════════════════

@router.get("/periodos")
async def listar_periodos(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Listar todos los períodos F14 de la org."""
    org_id = user.get("org_id")
    result = supabase.table("renta_periodos") \
        .select("*") \
        .eq("org_id", org_id) \
        .order("periodo", desc=True) \
        .execute()
    return {"periodos": result.data or []}


@router.post("/periodos")
async def crear_periodo(
    body: PeriodoCreate,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Crear o abrir un período F14."""
    org_id = user.get("org_id")

    existing = supabase.table("renta_periodos") \
        .select("id") \
        .eq("org_id", org_id) \
        .eq("periodo", body.periodo) \
        .execute()

    if existing.data:
        return {"periodo": existing.data[0], "action": "already_exists"}

    pago_cuenta = round(body.pago_cuenta_base * 0.0175, 2)
    result = supabase.table("renta_periodos").insert({
        "org_id": org_id,
        "periodo": body.periodo,
        "pago_cuenta_base": body.pago_cuenta_base,
        "pago_cuenta_monto": pago_cuenta,
    }).execute()

    return {"periodo": result.data[0] if result.data else None, "action": "created"}


@router.get("/periodos/{periodo}")
async def detalle_periodo(
    periodo: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Detalle de un período con totales y registros."""
    org_id = user.get("org_id")

    per = supabase.table("renta_periodos") \
        .select("*") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()

    if not per.data:
        raise HTTPException(status_code=404, detail=f"Período {periodo} no encontrado")

    retenciones = supabase.table("renta_retenciones") \
        .select("*") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .order("created_at") \
        .execute()

    return {
        "periodo": per.data[0],
        "retenciones": retenciones.data or [],
        "total_registros": len(retenciones.data or []),
    }


# ═══════════════════════════════════════════
# ENDPOINTS — RETENCIONES CRUD
# ═══════════════════════════════════════════

@router.get("/retenciones")
async def listar_retenciones(
    periodo: str = Query(..., description="MMYYYY"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user.get("org_id")
    result = supabase.table("renta_retenciones") \
        .select("*") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .order("created_at") \
        .execute()
    return {"retenciones": result.data or [], "total": len(result.data or [])}


@router.post("/retenciones")
async def crear_retencion(
    body: RetencionCreate,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user.get("org_id")
    data = body.dict()
    data["org_id"] = org_id

    result = supabase.table("renta_retenciones").insert(data).execute()
    _update_periodo_totals(supabase, org_id, body.periodo)

    return {"retencion": result.data[0] if result.data else None}


@router.put("/retenciones/{retencion_id}")
async def actualizar_retencion(
    retencion_id: str,
    body: RetencionUpdate,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user.get("org_id")
    update_data = {k: v for k, v in body.dict().items() if v is not None}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()

    result = supabase.table("renta_retenciones") \
        .update(update_data) \
        .eq("id", retencion_id) \
        .eq("org_id", org_id) \
        .execute()

    if result.data:
        _update_periodo_totals(supabase, org_id, result.data[0]["periodo"])

    return {"retencion": result.data[0] if result.data else None}


@router.delete("/retenciones/{retencion_id}")
async def eliminar_retencion(
    retencion_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user.get("org_id")

    existing = supabase.table("renta_retenciones") \
        .select("periodo") \
        .eq("id", retencion_id) \
        .eq("org_id", org_id) \
        .execute()

    supabase.table("renta_retenciones") \
        .delete() \
        .eq("id", retencion_id) \
        .eq("org_id", org_id) \
        .execute()

    if existing.data:
        _update_periodo_totals(supabase, org_id, existing.data[0]["periodo"])

    return {"deleted": True}


# ═══════════════════════════════════════════
# ENDPOINTS — CÁLCULO ISR
# ═══════════════════════════════════════════

@router.post("/calcular-isr")
async def calcular_isr(
    body: CalcularISRRequest,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Calcular ISR automático usando tabla progresiva."""
    afp = body.afp if body.afp is not None else round(body.salario_base * 0.0725, 2)
    isss = body.isss if body.isss is not None else round(min(body.salario_base, 1000) * 0.03, 2)
    isr = _calcular_isr_tabla(body.salario_base, afp, isss, supabase)
    base_imponible = body.salario_base - afp - isss

    return {
        "salario_base": body.salario_base,
        "afp": afp,
        "isss": isss,
        "base_imponible": round(base_imponible, 2),
        "isr_retenido": isr,
    }


# ═══════════════════════════════════════════
# ENDPOINTS — SYNC TIPO 07
# ═══════════════════════════════════════════

@router.post("/sync-tipo07")
async def sync_tipo07(
    periodo: str = Query(...),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Sincronizar DTEs tipo 07 del período al F14."""
    org_id = user.get("org_id")

    mes = int(periodo[:2])
    anio = int(periodo[2:])
    fecha_desde = f"{anio}-{mes:02d}-01"
    if mes == 12:
        fecha_hasta = f"{anio + 1}-01-01"
    else:
        fecha_hasta = f"{anio}-{mes + 1:02d}-01"

    dtes = supabase.table("dtes") \
        .select("id, json_data, estado") \
        .eq("org_id", org_id) \
        .eq("tipo_dte", "07") \
        .gte("fecha_emision", fecha_desde) \
        .lt("fecha_emision", fecha_hasta) \
        .execute()

    synced = 0
    skipped = 0

    for dte in (dtes.data or []):
        existing = supabase.table("renta_retenciones") \
            .select("id") \
            .eq("dte_id", dte["id"]) \
            .eq("org_id", org_id) \
            .execute()

        if existing.data:
            skipped += 1
            continue

        json_data = dte.get("json_data", {})
        if isinstance(json_data, str):
            import json
            json_data = json.loads(json_data)

        receptor = json_data.get("receptor", {})
        cuerpo = json_data.get("cuerpoDocumento", [])
        resumen = json_data.get("resumen", {})

        monto_total = 0
        retencion_isr = 0
        if isinstance(cuerpo, list):
            for item in cuerpo:
                monto_total += float(item.get("montoDescu", 0) or item.get("compra", 0) or 0)
                retencion_isr += float(item.get("isr", 0) or 0)
        elif isinstance(cuerpo, dict):
            monto_total = float(cuerpo.get("montoDescu", 0) or 0)
            retencion_isr = float(cuerpo.get("isr", 0) or 0)

        if retencion_isr == 0:
            retencion_isr = float(resumen.get("montoTotalOperacion", 0) or 0)

        nombre = receptor.get("nombre", "Sin nombre")
        nit = receptor.get("numDocumento", "")

        supabase.table("renta_retenciones").insert({
            "org_id": org_id,
            "periodo": periodo,
            "origen": "dte_07",
            "dte_id": dte["id"],
            "domicilio": 1,
            "codigo_pais": "9300",
            "nombre_razon": nombre,
            "nit_nif": nit,
            "codigo_ingreso": "11",
            "monto_devengado": monto_total,
            "impuesto_retenido": retencion_isr,
            "tipo_operacion": 1,
            "clasificacion": 2,
            "sector": 4,
            "tipo_costo_gasto": 1,
        }).execute()
        synced += 1

    _update_periodo_totals(supabase, org_id, periodo)

    return {
        "synced": synced,
        "skipped": skipped,
        "total_dtes_07": len(dtes.data or []),
    }


# ═══════════════════════════════════════════
# ENDPOINTS — EXPORT CSV F14
# ═══════════════════════════════════════════

@router.get("/export-csv/{periodo}")
async def export_csv_f14(
    periodo: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Generar CSV del F14 para upload al portal DGII."""
    org_id = user.get("org_id")

    retenciones = supabase.table("renta_retenciones") \
        .select("*") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .order("created_at") \
        .execute()

    if not retenciones.data:
        raise HTTPException(status_code=404, detail="No hay retenciones para este período")

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

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

    csv_content = output.getvalue()
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=F14_ANEXO_{periodo}.csv"}
    )


# ═══════════════════════════════════════════
# ENDPOINTS — RESUMEN
# ═══════════════════════════════════════════

@router.get("/resumen/{periodo}")
async def resumen_f14(
    periodo: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Resumen del F14: pago a cuenta + retenciones."""
    org_id = user.get("org_id")

    per = supabase.table("renta_periodos") \
        .select("*") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()

    retenciones = supabase.table("renta_retenciones") \
        .select("origen, codigo_ingreso, impuesto_retenido") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()

    data = retenciones.data or []
    total_isr = sum(float(r["impuesto_retenido"]) for r in data)
    by_origen = {}
    for r in data:
        o = r["origen"]
        by_origen[o] = by_origen.get(o, 0) + float(r["impuesto_retenido"])

    periodo_data = per.data[0] if per.data else {}
    pago_cuenta = float(periodo_data.get("pago_cuenta_monto", 0))

    return {
        "periodo": periodo,
        "pago_cuenta_base": float(periodo_data.get("pago_cuenta_base", 0)),
        "pago_cuenta_tasa": 0.0175,
        "pago_cuenta_monto": pago_cuenta,
        "total_retenciones": total_isr,
        "total_a_declarar": round(pago_cuenta + total_isr, 2),
        "registros": len(data),
        "por_origen": by_origen,
    }


# ═══════════════════════════════════════════
# ENDPOINTS — CATÁLOGOS
# ═══════════════════════════════════════════

@router.get("/codigos-ingreso")
async def listar_codigos_ingreso(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Catálogo de 46 códigos de ingreso MH con tasas."""
    result = supabase.table("renta_codigos_ingreso") \
        .select("*") \
        .eq("activo", True) \
        .order("codigo") \
        .execute()
    return {"codigos": result.data or []}


@router.get("/paises")
async def listar_paises(
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Catálogo de países MH."""
    result = supabase.table("renta_paises") \
        .select("*") \
        .order("codigo") \
        .execute()
    return {"paises": result.data or []}
