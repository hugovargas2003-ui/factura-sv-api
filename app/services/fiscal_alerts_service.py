"""
Fiscal Alerts Service — Genera alertas automáticas de obligaciones fiscales.
Usa notification_service existente para crear las notificaciones.
NO modifica notification_service.py.
"""
import logging
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)


def _get_business_day(year: int, month: int, target_day: int) -> date:
    """Get the Nth business day of a month (approx — skips weekends)."""
    count = 0
    d = date(year, month, 1)
    while count < target_day:
        if d.weekday() < 5:  # Monday-Friday
            count += 1
        if count < target_day:
            d += timedelta(days=1)
    return d


async def check_fiscal_alerts(supabase, org_id: str, user_id: str):
    """
    Check all fiscal obligations and create notifications for pending items.
    Returns list of alerts generated.
    """
    from app.services.notification_service import create_notification

    alerts = []
    today = date.today()

    # Current period (previous month — declarations are for the prior month)
    if today.month == 1:
        decl_month = 12
        decl_year = today.year - 1
    else:
        decl_month = today.month - 1
        decl_year = today.year

    periodo = f"{decl_month:02d}{decl_year}"

    # Deadline: 10th business day of current month
    deadline = _get_business_day(today.year, today.month, 10)
    days_left = (deadline - today).days

    # Date range for the declaration period
    fecha_desde = f"{decl_year}-{decl_month:02d}-01"
    if decl_month == 12:
        fecha_hasta = f"{decl_year + 1}-01-01"
    else:
        fecha_hasta = f"{decl_year}-{decl_month + 1:02d}-01"

    # ═══ ALERT 1: F-07 Deadline ═══
    if days_left <= 5 and days_left > 0:
        dtes = supabase.table("dtes") \
            .select("id", count="exact") \
            .eq("org_id", org_id) \
            .in_("estado", ["procesado", "IMPORTADO"]) \
            .gte("fecha_emision", fecha_desde) \
            .lt("fecha_emision", fecha_hasta) \
            .execute()

        dte_count = len(dtes.data or [])
        if dte_count > 0:
            alerts.append({
                "tipo": "warning",
                "titulo": f"F-07 vence en {days_left} día{'s' if days_left > 1 else ''}",
                "mensaje": f"La declaración F-07 de {periodo} tiene {dte_count} DTEs. Fecha límite: {deadline.strftime('%d/%m/%Y')}. Genere los anexos desde Reportes Fiscales.",
            })
    elif days_left == 0:
        alerts.append({
            "tipo": "warning",
            "titulo": "F-07 vence HOY",
            "mensaje": f"Hoy es el último día para presentar el F-07 de {periodo}. Vaya a Reportes Fiscales para generar los anexos.",
        })

    # ═══ ALERT 2: F-14 Deadline ═══
    renta = supabase.table("renta_periodos") \
        .select("status, total_registros") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()

    f14_status = renta.data[0].get("status", "missing") if renta.data else "missing"

    if days_left <= 5 and days_left > 0 and f14_status not in ("exported", "presented"):
        alerts.append({
            "tipo": "warning",
            "titulo": f"F-14 Renta vence en {days_left} día{'s' if days_left > 1 else ''}",
            "mensaje": f"La declaración F-14 de {periodo} está en estado '{f14_status}'. Vaya a Renta F-14 para completarla.",
        })

    # ═══ ALERT 3: Planilla no cargada ═══
    planilla = supabase.table("planilla_resumen") \
        .select("status") \
        .eq("org_id", org_id) \
        .eq("periodo", periodo) \
        .execute()

    if not planilla.data and days_left <= 7:
        alerts.append({
            "tipo": "info",
            "titulo": f"Planilla de {periodo} no ha sido cargada",
            "mensaje": "Suba la planilla mensual para generar automáticamente las líneas del F-14 y la partida contable.",
        })

    # ═══ ALERT 4: DTEs tipo 07 sin sincronizar al F14 ═══
    if renta.data:
        dtes_07 = supabase.table("dtes") \
            .select("id", count="exact") \
            .eq("org_id", org_id) \
            .eq("tipo_dte", "07") \
            .in_("estado", ["procesado", "IMPORTADO"]) \
            .gte("fecha_emision", fecha_desde) \
            .lt("fecha_emision", fecha_hasta) \
            .execute()

        synced = supabase.table("renta_retenciones") \
            .select("id", count="exact") \
            .eq("org_id", org_id) \
            .eq("periodo", periodo) \
            .eq("origen", "dte_07") \
            .execute()

        total_07 = len(dtes_07.data or [])
        total_synced = len(synced.data or [])

        if total_07 > total_synced:
            diff = total_07 - total_synced
            alerts.append({
                "tipo": "info",
                "titulo": f"{diff} retención(es) tipo 07 sin sincronizar al F-14",
                "mensaje": f"Tiene {total_07} DTEs tipo 07 en {periodo} pero solo {total_synced} están en el F-14.",
            })

    # ═══ ALERT 5: Créditos bajos ═══
    org = supabase.table("organizations") \
        .select("credit_balance") \
        .eq("id", org_id) \
        .execute()

    credits = org.data[0].get("credit_balance", 0) if org.data else 0
    if credits < 50 and credits > 0:
        alerts.append({
            "tipo": "warning",
            "titulo": f"Solo quedan {credits} créditos DTE",
            "mensaje": "Recargue créditos para evitar interrupción en la emisión de facturas.",
        })
    elif credits == 0:
        alerts.append({
            "tipo": "error",
            "titulo": "Sin créditos DTE — no puede emitir",
            "mensaje": "Su saldo de créditos es 0. Recargue desde Créditos DTE para continuar emitiendo.",
        })

    # ═══ ALERT 6: Cuadre IVA con diferencia significativa ═══
    try:
        ventas = supabase.table("dtes") \
            .select("tipo_dte, monto_total, total_gravada") \
            .eq("org_id", org_id) \
            .in_("estado", ["procesado", "IMPORTADO"]) \
            .gte("fecha_emision", fecha_desde) \
            .lt("fecha_emision", fecha_hasta) \
            .execute()

        iva_debito = 0.0
        for v in (ventas.data or []):
            gravada = float(v.get("total_gravada", 0) or 0)
            tipo = v.get("tipo_dte", "")
            if tipo == "03":
                iva_debito += gravada * 0.13
            elif tipo == "01":
                iva_debito += gravada - gravada / 1.13

        compras = supabase.table("dte_recibidos") \
            .select("iva_credito") \
            .eq("org_id", org_id) \
            .eq("status", "active") \
            .gte("fec_emi", fecha_desde) \
            .lt("fec_emi", fecha_hasta) \
            .execute()

        iva_credito = sum(float(c.get("iva_credito", 0)) for c in (compras.data or []))

        if iva_debito > 0 and iva_credito > 0:
            diferencia = iva_debito - iva_credito
            if abs(diferencia) > 100:
                alerts.append({
                    "tipo": "info",
                    "titulo": f"Cuadre IVA {periodo}: ${diferencia:,.2f} a pagar",
                    "mensaje": f"IVA Débito: ${iva_debito:,.2f} — IVA Crédito: ${iva_credito:,.2f}. Verifique en Cuadre IVA antes de declarar.",
                })
    except Exception as e:
        logger.warning(f"Error checking IVA cuadre: {e}")

    # Create notifications for each alert (avoid duplicates)
    created = 0
    for alert in alerts:
        try:
            existing = supabase.table("notifications") \
                .select("id") \
                .eq("org_id", org_id) \
                .eq("titulo", alert["titulo"]) \
                .eq("leida", False) \
                .execute()

            if not existing.data:
                await create_notification(
                    supabase,
                    org_id=org_id,
                    titulo=alert["titulo"],
                    mensaje=alert["mensaje"],
                    tipo=alert["tipo"],
                    user_id=user_id,
                )
                created += 1
        except Exception as e:
            logger.warning(f"Error creating fiscal alert: {e}")

    return {
        "alerts": alerts,
        "created": created,
        "periodo": periodo,
        "deadline": str(deadline),
        "days_left": days_left,
    }
