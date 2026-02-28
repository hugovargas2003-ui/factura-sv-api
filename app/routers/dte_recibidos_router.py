"""
dte_recibidos_router.py — Bandeja de DTEs Recibidos (compras)
Upload JSON → parseo automático → Libro de Compras → Cuadre IVA
"""

import json
import csv
import logging
from decimal import Decimal
from typing import Optional
from datetime import datetime, timezone
from io import StringIO

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from app.dependencies import get_supabase, get_current_user

logger = logging.getLogger("dte_recibidos")
router = APIRouter(prefix="/api/v1", tags=["dte-recibidos"])


def parse_dte_json(raw: dict) -> dict:
    ident = raw.get("identificacion", {})
    emisor = raw.get("emisor", {})
    resumen = raw.get("resumen", {})
    cuerpo = raw.get("cuerpoDocumento", [])
    sello = raw.get("selloRecepcion", {})

    dir_e = emisor.get("direccion", {})
    dir_str = dir_e.get("complemento", "") if isinstance(dir_e, dict) else str(dir_e or "")

    iva = Decimal("0")
    for t in (resumen.get("tributos") or []):
        if isinstance(t, dict) and t.get("codigo") == "20":
            iva = Decimal(str(t.get("valor", 0)))
            break

    gravada = Decimal(str(resumen.get("totalGravada", 0)))
    tipo = ident.get("tipoDte", "")

    if iva == 0 and gravada > 0:
        if tipo == "01":
            iva = (gravada - gravada / Decimal("1.13")).quantize(Decimal("0.01"))
        else:
            iva = (gravada * Decimal("0.13")).quantize(Decimal("0.01"))

    return {
        "codigo_generacion": ident.get("codigoGeneracion", ""),
        "numero_control": ident.get("numeroControl", ""),
        "sello_recepcion": sello.get("sello", "") if isinstance(sello, dict) else "",
        "tipo_dte": tipo,
        "version": ident.get("version", 3),
        "ambiente": ident.get("ambiente", "00"),
        "fec_emi": ident.get("fecEmi", ""),
        "hor_emi": ident.get("horEmi", ""),
        "emisor_nit": emisor.get("nit", ""),
        "emisor_nrc": emisor.get("nrc", ""),
        "emisor_nombre": emisor.get("nombre", ""),
        "emisor_nombre_comercial": emisor.get("nombreComercial"),
        "emisor_cod_actividad": emisor.get("codActividad"),
        "emisor_desc_actividad": emisor.get("descActividad"),
        "emisor_direccion": dir_str,
        "emisor_telefono": emisor.get("telefono"),
        "emisor_correo": emisor.get("correo"),
        "total_no_suj": float(resumen.get("totalNoSuj", 0)),
        "total_exenta": float(resumen.get("totalExenta", 0)),
        "total_gravada": float(gravada),
        "sub_total": float(resumen.get("subTotal", resumen.get("subTotalVentas", 0))),
        "iva_credito": float(iva),
        "iva_retenido": float(resumen.get("ivaRete1", 0)),
        "iva_percibido": float(resumen.get("ivaPerci1", 0)),
        "retencion_renta": float(resumen.get("reteRenta", 0)),
        "monto_total": float(resumen.get("montoTotalOperacion", resumen.get("totalPagar", 0))),
        "total_pagar": float(resumen.get("totalPagar", 0)),
        "condicion_operacion": resumen.get("condicionOperacion"),
        "items_count": len(cuerpo) if isinstance(cuerpo, list) else 0,
        "json_original": raw,
    }


@router.post("/dte-recibidos/upload")
async def upload_dte_recibidos(
    files: list[UploadFile] = File(...),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user["org_id"]
    uploaded, updated, errors = 0, 0, []

    for f in files:
        try:
            content = await f.read()
            raw = json.loads(content.decode("utf-8"))
            parsed = parse_dte_json(raw)

            if not parsed["codigo_generacion"]:
                errors.append({"file": f.filename, "error": "Sin codigo de generacion"})
                continue

            existing = supabase.table("dte_recibidos").select("id").eq(
                "org_id", org_id
            ).eq("codigo_generacion", parsed["codigo_generacion"]).execute()

            record = {**parsed, "org_id": org_id, "source": "manual_upload", "status": "active"}

            if existing.data:
                record["updated_at"] = datetime.now(timezone.utc).isoformat()
                supabase.table("dte_recibidos").update(record).eq("id", existing.data[0]["id"]).execute()
                updated += 1
            else:
                supabase.table("dte_recibidos").insert(record).execute()
                uploaded += 1
        except json.JSONDecodeError:
            errors.append({"file": f.filename, "error": "JSON invalido"})
        except Exception as e:
            errors.append({"file": f.filename, "error": str(e)[:200]})

    return {"uploaded": uploaded, "updated": updated, "errors": errors, "total_processed": uploaded + updated}


@router.get("/dte-recibidos")
async def list_dte_recibidos(
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    tipo_dte: Optional[str] = None,
    emisor: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user["org_id"]
    cols = "id,org_id,codigo_generacion,numero_control,sello_recepcion,tipo_dte,fec_emi,hor_emi,emisor_nit,emisor_nrc,emisor_nombre,emisor_nombre_comercial,total_gravada,total_exenta,total_no_suj,iva_credito,monto_total,condicion_operacion,items_count,source,status,created_at"

    q = supabase.table("dte_recibidos").select(cols).eq("org_id", org_id).eq("status", "active").order("fec_emi", desc=True)

    if fecha_desde:
        q = q.gte("fec_emi", fecha_desde)
    if fecha_hasta:
        q = q.lte("fec_emi", fecha_hasta)
    if tipo_dte:
        q = q.eq("tipo_dte", tipo_dte)
    if emisor:
        q = q.or_(f"emisor_nombre.ilike.%{emisor}%,emisor_nit.ilike.%{emisor}%")

    result = q.limit(limit).offset(offset).execute()

    count_q = supabase.table("dte_recibidos").select("id", count="exact").eq("org_id", org_id).eq("status", "active")
    if fecha_desde:
        count_q = count_q.gte("fec_emi", fecha_desde)
    if fecha_hasta:
        count_q = count_q.lte("fec_emi", fecha_hasta)
    if tipo_dte:
        count_q = count_q.eq("tipo_dte", tipo_dte)
    count_result = count_q.execute()

    return {"dte_recibidos": result.data or [], "total": count_result.count or 0, "limit": limit, "offset": offset}


@router.get("/dte-recibidos/resumen")
async def resumen_dte_recibidos(
    mes: int = Query(..., ge=1, le=12),
    anio: int = Query(..., ge=2020, le=2030),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user["org_id"]
    fecha_desde = f"{anio}-{mes:02d}-01"
    if mes == 12:
        fecha_hasta = f"{anio + 1}-01-01"
    else:
        fecha_hasta = f"{anio}-{mes + 1:02d}-01"

    rows = supabase.table("dte_recibidos").select(
        "tipo_dte,total_gravada,total_exenta,total_no_suj,iva_credito,iva_retenido,iva_percibido,retencion_renta,monto_total"
    ).eq("org_id", org_id).eq("status", "active").gte("fec_emi", fecha_desde).lt("fec_emi", fecha_hasta).execute()

    data = rows.data or []
    totales = {"documentos": len(data), "gravadas": 0, "exentas": 0, "no_suj": 0, "iva_credito": 0, "iva_retenido": 0, "iva_percibido": 0, "retencion_renta": 0, "total": 0}
    por_tipo = {}

    for r in data:
        totales["gravadas"] += float(r.get("total_gravada", 0))
        totales["exentas"] += float(r.get("total_exenta", 0))
        totales["no_suj"] += float(r.get("total_no_suj", 0))
        totales["iva_credito"] += float(r.get("iva_credito", 0))
        totales["iva_retenido"] += float(r.get("iva_retenido", 0))
        totales["iva_percibido"] += float(r.get("iva_percibido", 0))
        totales["retencion_renta"] += float(r.get("retencion_renta", 0))
        totales["total"] += float(r.get("monto_total", 0))
        t = r.get("tipo_dte", "?")
        por_tipo[t] = por_tipo.get(t, 0) + 1

    for k in totales:
        if isinstance(totales[k], float):
            totales[k] = round(totales[k], 2)

    return {"totales": totales, "por_tipo": por_tipo, "periodo": f"{anio}-{mes:02d}"}


@router.get("/dte-recibidos/libro-compras")
async def libro_compras(
    mes: int = Query(..., ge=1, le=12),
    anio: int = Query(..., ge=2020, le=2030),
    formato: str = Query("json"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user["org_id"]
    fecha_desde = f"{anio}-{mes:02d}-01"
    fecha_hasta = f"{anio + 1}-01-01" if mes == 12 else f"{anio}-{mes + 1:02d}-01"

    rows = supabase.table("dte_recibidos").select("*").eq("org_id", org_id).eq(
        "status", "active"
    ).gte("fec_emi", fecha_desde).lt("fec_emi", fecha_hasta).order("fec_emi").execute()

    TIPO_NAMES = {"01": "Factura", "03": "CCF", "05": "NC", "06": "ND", "11": "FEXE", "14": "FSE", "07": "CRE", "08": "CLE", "09": "DCLE", "15": "CDE"}
    entries = []
    totales = {"gravadas": 0, "exentas": 0, "no_suj": 0, "iva_credito": 0, "iva_retenido": 0, "iva_percibido": 0, "total": 0}

    for i, r in enumerate(rows.data or [], 1):
        entry = {
            "correlativo": i,
            "fecha": r.get("fec_emi", ""),
            "clase_doc": TIPO_NAMES.get(r.get("tipo_dte", ""), r.get("tipo_dte", "")),
            "numero_doc": r.get("numero_control") or r.get("codigo_generacion", "")[:20],
            "nrc_proveedor": r.get("emisor_nrc", ""),
            "nombre_proveedor": r.get("emisor_nombre", ""),
            "compras_exentas": float(r.get("total_exenta", 0)),
            "compras_gravadas": float(r.get("total_gravada", 0)),
            "credito_fiscal": float(r.get("iva_credito", 0)),
            "sujetos_excluidos": float(r.get("total_no_suj", 0)),
            "total_compras": float(r.get("monto_total", 0)),
            "retencion_iva": float(r.get("iva_retenido", 0)),
            "percepcion_iva": float(r.get("iva_percibido", 0)),
        }
        entries.append(entry)
        for k in ["gravadas", "exentas", "no_suj", "iva_credito", "iva_retenido", "iva_percibido", "total"]:
            map_key = {"gravadas": "compras_gravadas", "exentas": "compras_exentas", "no_suj": "sujetos_excluidos", "iva_credito": "credito_fiscal", "iva_retenido": "retencion_iva", "iva_percibido": "percepcion_iva", "total": "total_compras"}
            totales[k] += entry[map_key[k]]

    for k in totales:
        totales[k] = round(totales[k], 2)

    if formato == "csv":
        output = StringIO()
        writer = csv.writer(output, delimiter=";")
        for e in entries:
            writer.writerow([e["correlativo"], e["fecha"], e["clase_doc"], e["numero_doc"], e["nrc_proveedor"], e["nombre_proveedor"], e["compras_exentas"], e["compras_gravadas"], e["credito_fiscal"], e["sujetos_excluidos"], e["total_compras"], e["retencion_iva"], e["percepcion_iva"]])
        from fastapi.responses import Response
        return Response(content=output.getvalue(), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=libro_compras_{anio}_{mes:02d}.csv"})

    return {"periodo": f"{anio}-{mes:02d}", "entries": entries, "totales": totales, "count": len(entries)}


@router.get("/dte-recibidos/cuadre-iva")
async def cuadre_iva(
    mes: int = Query(..., ge=1, le=12),
    anio: int = Query(..., ge=2020, le=2030),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    org_id = user["org_id"]
    fecha_desde = f"{anio}-{mes:02d}-01"
    fecha_hasta = f"{anio + 1}-01-01" if mes == 12 else f"{anio}-{mes + 1:02d}-01"

    # IVA Débito: DTEs emitidos (tabla "dtes")
    emitidos = supabase.table("dtes").select(
        "tipo_dte,total_gravada,monto_total"
    ).eq("org_id", org_id).eq("estado", "procesado").gte("fecha_emision", fecha_desde).lt("fecha_emision", fecha_hasta).execute()

    iva_debito = 0.0
    ventas_gravadas = 0.0
    ventas_total = 0.0
    for r in (emitidos.data or []):
        tipo = r.get("tipo_dte", "")
        gravada = float(r.get("total_gravada", 0))
        ventas_gravadas += gravada
        ventas_total += float(r.get("monto_total", 0))
        if tipo == "03":
            iva_debito += gravada * 0.13
        elif tipo == "01":
            iva_debito += gravada - gravada / 1.13
        elif tipo == "05":
            iva_debito -= gravada * 0.13

    # IVA Crédito: DTEs recibidos
    recibidos = supabase.table("dte_recibidos").select(
        "iva_credito,total_gravada,monto_total"
    ).eq("org_id", org_id).eq("status", "active").gte("fec_emi", fecha_desde).lt("fec_emi", fecha_hasta).execute()

    iva_credito = 0.0
    compras_gravadas = 0.0
    compras_total = 0.0
    for r in (recibidos.data or []):
        iva_credito += float(r.get("iva_credito", 0))
        compras_gravadas += float(r.get("total_gravada", 0))
        compras_total += float(r.get("monto_total", 0))

    diferencia = round(iva_debito - iva_credito, 2)
    return {
        "periodo": f"{anio}-{mes:02d}",
        "ventas": {"gravadas": round(ventas_gravadas, 2), "total": round(ventas_total, 2), "iva_debito": round(iva_debito, 2), "dtes_emitidos": len(emitidos.data or [])},
        "compras": {"gravadas": round(compras_gravadas, 2), "total": round(compras_total, 2), "iva_credito": round(iva_credito, 2), "dtes_recibidos": len(recibidos.data or [])},
        "cuadre": {"iva_debito": round(iva_debito, 2), "iva_credito": round(iva_credito, 2), "diferencia": diferencia, "resultado": "A PAGAR" if diferencia > 0 else "REMANENTE A FAVOR"},
    }


@router.get("/dte-recibidos/{dte_id}")
async def get_dte_recibido(
    dte_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    result = supabase.table("dte_recibidos").select("*").eq("id", dte_id).eq("org_id", user["org_id"]).single().execute()
    if not result.data:
        raise HTTPException(404, "DTE recibido no encontrado")
    return result.data


@router.delete("/dte-recibidos/{dte_id}")
async def delete_dte_recibido(
    dte_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    supabase.table("dte_recibidos").update({"status": "anulado", "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", dte_id).eq("org_id", user["org_id"]).execute()
    return {"deleted": True, "id": dte_id}
