"""
cxp_service.py — Cuentas por Pagar (Accounts Payable).

Location: app/services/cxp_service.py
NEW FILE — mirrors cxc_service.py pattern for supplier invoices.
"""

import json
from datetime import datetime, date, timedelta
from typing import Any, Optional


def _today() -> str:
    return date.today().isoformat()


async def list_cxp(
    supabase: Any,
    org_id: str,
    estado_pago: Optional[str] = None,
    proveedor: Optional[str] = None,
    vencido: Optional[bool] = None,
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """List accounts payable with filters."""
    query = (
        supabase.table("cuentas_por_pagar")
        .select(
            "id, proveedor_nombre, proveedor_nit, numero_factura, "
            "descripcion, fecha_factura, fecha_vencimiento, "
            "monto_total, monto_pagado, estado_pago, categoria, pagos",
            count="exact",
        )
        .eq("org_id", org_id)
        .order("fecha_vencimiento", desc=False)
    )

    if estado_pago:
        query = query.eq("estado_pago", estado_pago)
    if proveedor:
        query = query.ilike("proveedor_nombre", f"%{proveedor}%")
    if vencido is True:
        query = query.lt("fecha_vencimiento", _today()).neq("estado_pago", "pagado")

    offset = (page - 1) * per_page
    query = query.range(offset, offset + per_page - 1)
    result = query.execute()

    return {
        "data": result.data or [],
        "total": result.count or 0,
        "page": page,
        "per_page": per_page,
    }


async def create_cxp(
    supabase: Any,
    org_id: str,
    user_id: str,
    data: dict,
) -> dict:
    """Create a new account payable."""
    record = {
        "org_id": org_id,
        "created_by": user_id,
        "proveedor_nombre": data["proveedor_nombre"],
        "proveedor_nit": data.get("proveedor_nit"),
        "proveedor_nrc": data.get("proveedor_nrc"),
        "proveedor_correo": data.get("proveedor_correo"),
        "proveedor_telefono": data.get("proveedor_telefono"),
        "descripcion": data.get("descripcion"),
        "numero_factura": data.get("numero_factura"),
        "fecha_factura": data.get("fecha_factura", _today()),
        "fecha_vencimiento": data.get("fecha_vencimiento"),
        "monto_total": float(data["monto_total"]),
        "monto_pagado": 0,
        "estado_pago": "pendiente",
        "categoria": data.get("categoria", "general"),
        "notas": data.get("notas"),
        "pagos": [],
    }

    result = supabase.table("cuentas_por_pagar").insert(record).execute()
    return result.data[0] if result.data else record


async def register_payment(
    supabase: Any,
    org_id: str,
    cxp_id: str,
    monto: float,
    metodo: str = "efectivo",
    referencia: str = "",
    nota: str = "",
) -> dict:
    """Register a full or partial payment on a CxP."""
    result = supabase.table("cuentas_por_pagar").select(
        "id, monto_total, monto_pagado, estado_pago, pagos"
    ).eq("id", cxp_id).eq("org_id", org_id).single().execute()

    if not result.data:
        raise ValueError("Cuenta por pagar no encontrada")

    cxp = result.data
    monto_total = float(cxp["monto_total"] or 0)
    monto_pagado = float(cxp["monto_pagado"] or 0)
    pagos_prev = cxp.get("pagos") or []

    if monto <= 0:
        raise ValueError("Monto debe ser mayor a 0")

    nuevo_pagado = monto_pagado + monto
    if nuevo_pagado > monto_total + 0.01:
        raise ValueError(
            f"El pago excede el saldo. Total: ${monto_total:.2f}, "
            f"Pagado: ${monto_pagado:.2f}, Saldo: ${monto_total - monto_pagado:.2f}"
        )

    if abs(nuevo_pagado - monto_total) < 0.01:
        nuevo_estado = "pagado"
    else:
        nuevo_estado = "parcial"

    pago_record = {
        "monto": monto,
        "metodo": metodo,
        "referencia": referencia,
        "nota": nota,
        "fecha": _today(),
        "timestamp": datetime.utcnow().isoformat(),
    }

    nuevos_pagos = pagos_prev + [pago_record]

    supabase.table("cuentas_por_pagar").update({
        "monto_pagado": nuevo_pagado,
        "estado_pago": nuevo_estado,
        "pagos": nuevos_pagos,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", cxp_id).execute()

    return {
        "success": True,
        "cxp_id": cxp_id,
        "pago": pago_record,
        "monto_pagado": nuevo_pagado,
        "saldo_pendiente": monto_total - nuevo_pagado,
        "estado_pago": nuevo_estado,
    }


async def delete_cxp(
    supabase: Any,
    org_id: str,
    cxp_id: str,
) -> dict:
    """Delete a CxP (only if pendiente and no payments)."""
    result = supabase.table("cuentas_por_pagar").select(
        "id, estado_pago, monto_pagado"
    ).eq("id", cxp_id).eq("org_id", org_id).single().execute()

    if not result.data:
        raise ValueError("Cuenta por pagar no encontrada")

    if float(result.data.get("monto_pagado", 0)) > 0:
        raise ValueError("No se puede eliminar una cuenta con pagos registrados")

    supabase.table("cuentas_por_pagar").delete().eq("id", cxp_id).eq("org_id", org_id).execute()
    return {"success": True, "deleted": cxp_id}


async def get_aging_report(supabase: Any, org_id: str) -> dict:
    """Aging report for accounts payable."""
    result = supabase.table("cuentas_por_pagar").select(
        "id, proveedor_nombre, proveedor_nit, numero_factura, "
        "monto_total, monto_pagado, fecha_factura, fecha_vencimiento, estado_pago"
    ).eq("org_id", org_id).in_(
        "estado_pago", ["pendiente", "parcial"]
    ).execute()

    rows = result.data or []
    hoy = date.today()

    buckets = {
        "vigente": {"label": "Vigente", "items": [], "total": 0, "count": 0},
        "1_30": {"label": "1-30 dias", "items": [], "total": 0, "count": 0},
        "31_60": {"label": "31-60 dias", "items": [], "total": 0, "count": 0},
        "61_90": {"label": "61-90 dias", "items": [], "total": 0, "count": 0},
        "90_plus": {"label": "90+ dias", "items": [], "total": 0, "count": 0},
    }

    total_pendiente = 0.0

    for row in rows:
        saldo = float(row["monto_total"] or 0) - float(row["monto_pagado"] or 0)
        if saldo <= 0:
            continue

        total_pendiente += saldo
        fv = row.get("fecha_vencimiento")

        if not fv:
            bucket_key = "vigente"
        else:
            try:
                fecha_venc = date.fromisoformat(fv) if isinstance(fv, str) else fv
                dias_vencido = (hoy - fecha_venc).days
            except (ValueError, TypeError):
                dias_vencido = 0

            if dias_vencido <= 0:
                bucket_key = "vigente"
            elif dias_vencido <= 30:
                bucket_key = "1_30"
            elif dias_vencido <= 60:
                bucket_key = "31_60"
            elif dias_vencido <= 90:
                bucket_key = "61_90"
            else:
                bucket_key = "90_plus"

        entry = {
            "id": row["id"],
            "proveedor_nombre": row["proveedor_nombre"],
            "proveedor_nit": row.get("proveedor_nit", ""),
            "numero_factura": row.get("numero_factura", ""),
            "monto_total": float(row["monto_total"] or 0),
            "saldo": saldo,
            "fecha_factura": row["fecha_factura"],
            "fecha_vencimiento": fv,
        }
        buckets[bucket_key]["items"].append(entry)
        buckets[bucket_key]["total"] += saldo
        buckets[bucket_key]["count"] += 1

    return {
        "total_pendiente": round(total_pendiente, 2),
        "total_cxp": sum(b["count"] for b in buckets.values()),
        "buckets": buckets,
    }


async def get_cxp_stats(supabase: Any, org_id: str) -> dict:
    """Dashboard stats for CxP."""
    result = supabase.table("cuentas_por_pagar").select(
        "estado_pago, monto_total, monto_pagado"
    ).eq("org_id", org_id).execute()

    rows = result.data or []

    total_comprometido = 0.0
    total_pagado = 0.0
    pendiente_count = 0
    pagado_count = 0
    parcial_count = 0

    for r in rows:
        mt = float(r.get("monto_total", 0) or 0)
        mp = float(r.get("monto_pagado", 0) or 0)
        ep = r.get("estado_pago", "pendiente")

        total_comprometido += mt
        total_pagado += mp

        if ep == "pagado":
            pagado_count += 1
        elif ep == "parcial":
            parcial_count += 1
        else:
            pendiente_count += 1

    return {
        "total_comprometido": round(total_comprometido, 2),
        "total_pagado": round(total_pagado, 2),
        "total_pendiente": round(total_comprometido - total_pagado, 2),
        "tasa_pago": round(
            (total_pagado / total_comprometido * 100) if total_comprometido > 0 else 0, 1
        ),
        "cxp_pendientes": pendiente_count,
        "cxp_parciales": parcial_count,
        "cxp_pagados": pagado_count,
    }
