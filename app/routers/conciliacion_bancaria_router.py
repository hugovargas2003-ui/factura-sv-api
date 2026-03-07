"""
Conciliación Bancaria Router — Upload estado de cuenta + auto-match con DTEs.
Usa tabla conciliacion_movimientos_banco (creada en Fase A).
NO modifica reconciliacion_router.py existente (esa es POS vs DTE).
"""
import logging
import io
import csv
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/conciliacion-bancaria", tags=["conciliacion-bancaria"])

from app.dependencies import get_current_user, get_supabase


@router.post("/upload")
async def upload_estado_cuenta(
    banco: str = Query("generico", description="BAC, agricola, davivienda, generico"),
    file: UploadFile = File(...),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Upload CSV de estado de cuenta bancario."""
    org_id = user.get("org_id")
    content = await file.read()
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    created = 0
    errors = []

    for i, row in enumerate(rows):
        try:
            fecha = None
            monto = None
            referencia = None
            descripcion = None

            for key, val in row.items():
                k = str(key).strip().lower()
                v = str(val).strip() if val else ""
                if not v:
                    continue

                if k in ("fecha", "date", "fecha_transaccion", "fecha transaccion"):
                    fecha = v
                elif k in ("monto", "amount", "valor", "importe"):
                    try:
                        monto = float(v.replace(",", "").replace("$", ""))
                    except ValueError:
                        pass
                elif k in ("referencia", "reference", "ref", "numero", "no_transaccion"):
                    referencia = v
                elif k in ("descripcion", "description", "concepto", "detalle"):
                    descripcion = v

            # Try separate debit/credit columns
            if monto is None:
                for key, val in row.items():
                    k = str(key).strip().lower()
                    v = str(val).strip() if val else ""
                    if k in ("debito", "debit", "cargo") and v:
                        try:
                            monto = -abs(float(v.replace(",", "").replace("$", "")))
                        except ValueError:
                            pass
                    elif k in ("credito", "credit", "abono", "deposito") and v:
                        try:
                            monto = abs(float(v.replace(",", "").replace("$", "")))
                        except ValueError:
                            pass

            if monto is None:
                errors.append(f"Fila {i+2}: sin monto detectable")
                continue

            tipo = "deposito" if monto > 0 else "retiro"

            supabase.table("conciliacion_movimientos_banco").insert({
                "org_id": org_id,
                "fecha": fecha or datetime.now().strftime("%Y-%m-%d"),
                "referencia": referencia,
                "descripcion": descripcion,
                "monto": monto,
                "tipo": tipo,
                "banco": banco,
                "status": "pending",
            }).execute()
            created += 1

        except Exception as e:
            errors.append(f"Fila {i+2}: {str(e)}")

    return {"created": created, "errors": errors, "total_rows": len(rows)}


@router.post("/auto-match")
async def auto_match(
    fecha_desde: str = Query(None),
    fecha_hasta: str = Query(None),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Match automático: movimientos bancarios vs DTEs por monto + fecha."""
    org_id = user.get("org_id")

    query = supabase.table("conciliacion_movimientos_banco") \
        .select("*").eq("org_id", org_id).eq("status", "pending")
    if fecha_desde:
        query = query.gte("fecha", fecha_desde)
    if fecha_hasta:
        query = query.lte("fecha", fecha_hasta)
    movimientos = query.execute()

    matched = 0
    for mov in (movimientos.data or []):
        monto_abs = abs(float(mov["monto"]))
        fecha = mov["fecha"]

        dtes = supabase.table("dtes") \
            .select("id, total_pagar, fecha_emision") \
            .eq("org_id", org_id) \
            .gte("fecha_emision", fecha[:10] if fecha else "2020-01-01") \
            .execute()

        best_match = None
        best_score = 0

        for dte in (dtes.data or []):
            dte_total = abs(float(dte.get("total_pagar", 0) or 0))
            if dte_total == 0:
                continue

            amount_diff = abs(monto_abs - dte_total)
            if amount_diff <= 0.05:
                score = 100
            elif amount_diff <= 1.00:
                score = 80
            elif amount_diff <= 5.00:
                score = 60
            else:
                continue

            if score > best_score:
                best_score = score
                best_match = dte

        if best_match and best_score >= 60:
            supabase.table("conciliacion_movimientos_banco") \
                .update({
                    "matched_dte_id": best_match["id"],
                    "matched_score": best_score,
                    "status": "matched",
                }).eq("id", mov["id"]).execute()
            matched += 1

    return {
        "total_movimientos": len(movimientos.data or []),
        "matched": matched,
        "unmatched": len(movimientos.data or []) - matched,
    }


@router.put("/match-manual")
async def match_manual(
    movimiento_id: str = Query(...),
    dte_id: str = Query(...),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Match manual: vincular un movimiento bancario a un DTE específico."""
    org_id = user.get("org_id")

    result = supabase.table("conciliacion_movimientos_banco") \
        .update({
            "matched_dte_id": dte_id,
            "matched_score": 100,
            "status": "manual",
        }).eq("id", movimiento_id).eq("org_id", org_id).execute()

    return {"updated": True, "movimiento": result.data[0] if result.data else None}


@router.get("/movimientos")
async def listar_movimientos(
    status: Optional[str] = None,
    banco: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Listar movimientos bancarios con filtros."""
    org_id = user.get("org_id")

    query = supabase.table("conciliacion_movimientos_banco") \
        .select("*").eq("org_id", org_id)
    if status:
        query = query.eq("status", status)
    if banco:
        query = query.eq("banco", banco)
    if fecha_desde:
        query = query.gte("fecha", fecha_desde)
    if fecha_hasta:
        query = query.lte("fecha", fecha_hasta)

    result = query.order("fecha", desc=True).limit(500).execute()
    return {"movimientos": result.data or [], "total": len(result.data or [])}


@router.get("/reporte")
async def reporte_conciliacion(
    fecha_desde: str = Query(None),
    fecha_hasta: str = Query(None),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Reporte de conciliación: totales matched/unmatched/discrepancias."""
    org_id = user.get("org_id")

    query = supabase.table("conciliacion_movimientos_banco") \
        .select("*").eq("org_id", org_id)
    if fecha_desde:
        query = query.gte("fecha", fecha_desde)
    if fecha_hasta:
        query = query.lte("fecha", fecha_hasta)

    movs = query.execute()
    data = movs.data or []

    total_depositos = sum(float(m["monto"]) for m in data if float(m["monto"]) > 0)
    total_retiros = sum(abs(float(m["monto"])) for m in data if float(m["monto"]) < 0)
    matched = [m for m in data if m["status"] in ("matched", "manual")]
    unmatched = [m for m in data if m["status"] == "pending"]

    return {
        "total_movimientos": len(data),
        "total_depositos": round(total_depositos, 2),
        "total_retiros": round(total_retiros, 2),
        "matched": len(matched),
        "unmatched": len(unmatched),
        "health_pct": round(len(matched) / max(len(data), 1) * 100, 1),
        "monto_no_conciliado": round(sum(abs(float(m["monto"])) for m in unmatched), 2),
    }


@router.delete("/movimientos/{movimiento_id}")
async def eliminar_movimiento(
    movimiento_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user.get("org_id")
    supabase.table("conciliacion_movimientos_banco") \
        .delete().eq("id", movimiento_id).eq("org_id", org_id).execute()
    return {"deleted": True}
