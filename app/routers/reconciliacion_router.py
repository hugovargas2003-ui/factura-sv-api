"""
reconciliacion_router.py — Reconciliación Fiscal POS vs DTE
Upload CSV → matching automático → discrepancias → health score
"""

import csv
import json
import logging
from typing import Optional
from datetime import datetime, timezone
from io import StringIO

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from app.dependencies import get_supabase, get_current_user

logger = logging.getLogger("reconciliacion")
router = APIRouter(prefix="/api/v1", tags=["reconciliacion"])


def parse_pos_csv(content: str) -> list:
    reader = csv.DictReader(StringIO(content))
    fields = reader.fieldnames or []
    fecha_col = next((f for f in fields if any(k in f.lower() for k in ["fecha", "date"])), None)
    monto_col = next((f for f in fields if any(k in f.lower() for k in ["monto", "total", "amount", "valor"])), None)
    ref_col = next((f for f in fields if any(k in f.lower() for k in ["ref", "id", "numero", "number"])), None)
    desc_col = next((f for f in fields if any(k in f.lower() for k in ["desc", "concepto", "detalle"])), None)

    if not fecha_col or not monto_col:
        raise ValueError(f"CSV necesita columnas de fecha y monto. Encontradas: {fields}")

    rows = []
    for row in reader:
        try:
            monto_str = row[monto_col].replace(",", "").replace("$", "").strip()
            rows.append({
                "fecha": row[fecha_col].strip(),
                "monto": float(monto_str),
                "referencia": row.get(ref_col, "").strip() if ref_col else "",
                "descripcion": row.get(desc_col, "").strip() if desc_col else "",
            })
        except (ValueError, KeyError):
            continue
    return rows


def match_pos_vs_dte(pos_rows: list, dte_rows: list, tolerance: float = 0.05) -> dict:
    matched = []
    unmatched_pos = []
    unmatched_dte = list(dte_rows)

    for pos_tx in pos_rows:
        pos_fecha = pos_tx["fecha"]
        pos_monto = float(pos_tx["monto"])
        found = False

        for i, dte in enumerate(unmatched_dte):
            dte_fecha = str(dte.get("fecha_emision", ""))[:10]
            dte_monto = float(dte.get("monto_total", 0))
            if pos_fecha == dte_fecha and abs(pos_monto - dte_monto) <= tolerance:
                matched.append({"pos": pos_tx, "dte_id": dte.get("id"), "diff": round(pos_monto - dte_monto, 2)})
                unmatched_dte.pop(i)
                found = True
                break

        if not found:
            unmatched_pos.append({"tipo": "VENTA_SIN_DTE", "severidad": "alta", "fecha": pos_tx["fecha"], "monto": pos_tx["monto"], "referencia": pos_tx.get("referencia", ""), "riesgo": "Venta POS sin documento fiscal"})

    disc_dte = [{"tipo": "DTE_SIN_VENTA", "severidad": "media", "fecha": str(d.get("fecha_emision", ""))[:10], "monto": float(d.get("monto_total", 0)), "codigo": d.get("codigo_generacion", "")[:16], "riesgo": "DTE emitido sin transaccion POS"} for d in unmatched_dte]

    total = len(pos_rows)
    health = (len(matched) / total * 100) if total > 0 else 100

    return {
        "matched": len(matched),
        "unmatched_pos": len(unmatched_pos),
        "unmatched_dte": len(disc_dte),
        "health_pct": round(health, 2),
        "discrepancias": unmatched_pos + disc_dte,
        "monto_discrepancia": round(sum(d["monto"] for d in unmatched_pos), 2),
    }


@router.post("/reconciliacion/upload")
async def upload_reconciliacion(
    file: UploadFile = File(...),
    fecha_desde: str = Form(...),
    fecha_hasta: str = Form(...),
    nombre: str = Form(""),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user["org_id"]
    content = (await file.read()).decode("utf-8")
    pos_rows = parse_pos_csv(content)

    if not pos_rows:
        raise HTTPException(400, "CSV vacio o sin datos validos")

    dte_rows = supabase.table("dtes").select(
        "id,fecha_emision,monto_total,codigo_generacion,tipo_dte"
    ).eq("org_id", org_id).eq("estado", "procesado").gte("fecha_emision", fecha_desde).lte("fecha_emision", fecha_hasta).execute()

    result = match_pos_vs_dte(pos_rows, dte_rows.data or [])

    if not nombre:
        nombre = f"Reconciliacion {fecha_desde} a {fecha_hasta}"

    record = {
        "org_id": org_id,
        "nombre": nombre,
        "fecha_desde": fecha_desde,
        "fecha_hasta": fecha_hasta,
        "pos_data": pos_rows,
        "pos_total_rows": len(pos_rows),
        "pos_total_monto": round(sum(r["monto"] for r in pos_rows), 2),
        "matched": result["matched"],
        "unmatched_pos": result["unmatched_pos"],
        "unmatched_dte": result["unmatched_dte"],
        "monto_discrepancia": result["monto_discrepancia"],
        "discrepancias": result["discrepancias"],
        "health_pct": result["health_pct"],
        "status": "completed",
    }
    insert = supabase.table("reconciliaciones").insert(record).execute()

    return {**result, "id": insert.data[0]["id"] if insert.data else None, "nombre": nombre, "pos_total_rows": len(pos_rows)}


@router.get("/reconciliacion")
async def list_reconciliaciones(
    limit: int = Query(20, ge=1, le=100),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    result = supabase.table("reconciliaciones").select(
        "id,nombre,fecha_desde,fecha_hasta,pos_total_rows,matched,unmatched_pos,unmatched_dte,health_pct,monto_discrepancia,status,created_at"
    ).eq("org_id", user["org_id"]).order("created_at", desc=True).limit(limit).execute()
    return {"reconciliaciones": result.data or []}


@router.get("/reconciliacion/{recon_id}")
async def get_reconciliacion(
    recon_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    result = supabase.table("reconciliaciones").select("*").eq("id", recon_id).eq("org_id", user["org_id"]).single().execute()
    if not result.data:
        raise HTTPException(404, "Reconciliacion no encontrada")
    return result.data


@router.delete("/reconciliacion/{recon_id}")
async def delete_reconciliacion(
    recon_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    supabase.table("reconciliaciones").delete().eq("id", recon_id).eq("org_id", user["org_id"]).execute()
    return {"deleted": True, "id": recon_id}
