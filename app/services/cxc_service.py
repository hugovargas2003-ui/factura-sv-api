"""
cxc_service.py — Cuentas por Cobrar (Accounts Receivable).

Location: app/services/cxc_service.py
NEW FILE — does not modify any existing infrastructure.

Features:
- List pending/overdue invoices
- Register full/partial payments
- Aging report (30/60/90 days)
- CxC dashboard stats
"""

import json
from datetime import datetime, date, timedelta
from typing import Any, Optional


def _today() -> str:
    return date.today().isoformat()


async def get_cxc_list(
    supabase: Any,
    org_id: str,
    estado_pago: Optional[str] = None,
    receptor_nit: Optional[str] = None,
    vencido: Optional[bool] = None,
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """List DTEs with payment status for CxC tracking."""
    query = (
        supabase.table("dtes")
        .select(
            "id, tipo_dte, numero_control, fecha_emision, "
            "receptor_nombre, receptor_nit, receptor_correo, "
            "monto_total, estado_pago, monto_pagado, "
            "fecha_vencimiento, pagos",
            count="exact",
        )
        .eq("org_id", org_id)
        .eq("estado", "PROCESADO")
        .order("fecha_vencimiento", desc=False)
    )

    if estado_pago:
        query = query.eq("estado_pago", estado_pago)
    if receptor_nit:
        query = query.eq("receptor_nit", receptor_nit)
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


async def register_payment(
    supabase: Any,
    org_id: str,
    dte_id: str,
    monto: float,
    metodo: str = "efectivo",
    referencia: str = "",
    nota: str = "",
) -> dict:
    """Register a full or partial payment on a DTE."""
    # Fetch DTE
    result = supabase.table("dtes").select(
        "id, monto_total, monto_pagado, estado_pago, pagos"
    ).eq("id", dte_id).eq("org_id", org_id).single().execute()

    if not result.data:
        raise ValueError("DTE no encontrado")

    dte = result.data
    monto_total = float(dte["monto_total"] or 0)
    monto_pagado = float(dte["monto_pagado"] or 0)
    pagos_prev = dte.get("pagos") or []

    if monto <= 0:
        raise ValueError("Monto debe ser mayor a 0")

    nuevo_pagado = monto_pagado + monto
    if nuevo_pagado > monto_total + 0.01:
        raise ValueError(
            f"El pago excede el saldo. Total: ${monto_total:.2f}, "
            f"Pagado: ${monto_pagado:.2f}, Saldo: ${monto_total - monto_pagado:.2f}"
        )

    # Determine new status
    if abs(nuevo_pagado - monto_total) < 0.01:
        nuevo_estado = "pagado"
    else:
        nuevo_estado = "parcial"

    # Build payment record
    pago_record = {
        "monto": monto,
        "metodo": metodo,
        "referencia": referencia,
        "nota": nota,
        "fecha": _today(),
        "timestamp": datetime.utcnow().isoformat(),
    }

    nuevos_pagos = pagos_prev + [pago_record]

    # Update DTE
    supabase.table("dtes").update({
        "monto_pagado": nuevo_pagado,
        "estado_pago": nuevo_estado,
        "pagos": nuevos_pagos,
    }).eq("id", dte_id).execute()

    return {
        "success": True,
        "dte_id": dte_id,
        "pago": pago_record,
        "monto_pagado": nuevo_pagado,
        "saldo_pendiente": monto_total - nuevo_pagado,
        "estado_pago": nuevo_estado,
    }


async def get_aging_report(supabase: Any, org_id: str) -> dict:
    """
    Aging report: group unpaid DTEs by overdue buckets.
    Buckets: vigente, 1-30 días, 31-60 días, 61-90 días, 90+ días
    """
    result = supabase.table("dtes").select(
        "id, tipo_dte, numero_control, fecha_emision, "
        "receptor_nombre, receptor_nit, "
        "monto_total, monto_pagado, fecha_vencimiento, estado_pago"
    ).eq("org_id", org_id).eq(
        "estado", "PROCESADO"
    ).in_("estado_pago", ["pendiente", "parcial"]).execute()

    rows = result.data or []
    hoy = date.today()

    buckets = {
        "vigente": {"label": "Vigente", "dtes": [], "total": 0, "count": 0},
        "1_30": {"label": "1-30 días", "dtes": [], "total": 0, "count": 0},
        "31_60": {"label": "31-60 días", "dtes": [], "total": 0, "count": 0},
        "61_90": {"label": "61-90 días", "dtes": [], "total": 0, "count": 0},
        "90_plus": {"label": "90+ días", "dtes": [], "total": 0, "count": 0},
    }

    total_pendiente = 0.0

    for dte in rows:
        saldo = float(dte["monto_total"] or 0) - float(dte["monto_pagado"] or 0)
        if saldo <= 0:
            continue

        total_pendiente += saldo
        fv = dte.get("fecha_vencimiento")

        if not fv:
            # No due date → treat as current
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

        dte_entry = {
            "id": dte["id"],
            "tipo_dte": dte["tipo_dte"],
            "numero_control": dte["numero_control"],
            "receptor_nombre": dte["receptor_nombre"],
            "receptor_nit": dte.get("receptor_nit", ""),
            "monto_total": float(dte["monto_total"] or 0),
            "saldo": saldo,
            "fecha_emision": dte["fecha_emision"],
            "fecha_vencimiento": fv,
        }
        buckets[bucket_key]["dtes"].append(dte_entry)
        buckets[bucket_key]["total"] += saldo
        buckets[bucket_key]["count"] += 1

    return {
        "total_pendiente": round(total_pendiente, 2),
        "total_dtes": sum(b["count"] for b in buckets.values()),
        "buckets": buckets,
    }


async def get_cxc_stats(supabase: Any, org_id: str) -> dict:
    """Dashboard stats for CxC."""
    result = supabase.table("dtes").select(
        "estado_pago, monto_total, monto_pagado"
    ).eq("org_id", org_id).eq("estado", "PROCESADO").execute()

    rows = result.data or []

    total_facturado = 0.0
    total_cobrado = 0.0
    pendiente_count = 0
    pagado_count = 0
    parcial_count = 0

    for r in rows:
        mt = float(r.get("monto_total", 0) or 0)
        mp = float(r.get("monto_pagado", 0) or 0)
        ep = r.get("estado_pago", "pendiente")

        total_facturado += mt
        total_cobrado += mp

        if ep == "pagado":
            pagado_count += 1
        elif ep == "parcial":
            parcial_count += 1
        else:
            pendiente_count += 1

    return {
        "total_facturado": round(total_facturado, 2),
        "total_cobrado": round(total_cobrado, 2),
        "total_pendiente": round(total_facturado - total_cobrado, 2),
        "tasa_cobro": round(
            (total_cobrado / total_facturado * 100) if total_facturado > 0 else 0, 1
        ),
        "dtes_pendientes": pendiente_count,
        "dtes_parciales": parcial_count,
        "dtes_pagados": pagado_count,
    }
