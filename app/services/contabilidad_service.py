"""
contabilidad_service.py — Basic accounting: chart of accounts + journal entries.

Location: app/services/contabilidad_service.py
NEW FILE — auto-generates journal entries from DTE emission.
Follows NIIF PYMES standards for El Salvador.
"""

import logging
from datetime import date
from typing import Any, Optional

logger = logging.getLogger("contabilidad_service")

# Default chart of accounts for El Salvador NIIF PYMES
DEFAULT_ACCOUNTS = [
    # Activos
    ("1", "ACTIVO", "activo", "deudora", None, 1),
    ("11", "Activo Corriente", "activo", "deudora", "1", 2),
    ("1101", "Efectivo y Equivalentes", "activo", "deudora", "11", 3),
    ("110101", "Caja General", "activo", "deudora", "1101", 4),
    ("110102", "Bancos", "activo", "deudora", "1101", 4),
    ("1102", "Cuentas por Cobrar Comerciales", "activo", "deudora", "11", 3),
    ("110201", "Clientes", "activo", "deudora", "1102", 4),
    ("1103", "Inventarios", "activo", "deudora", "11", 3),
    ("110301", "Mercaderias", "activo", "deudora", "1103", 4),
    ("1104", "IVA Credito Fiscal", "activo", "deudora", "11", 3),
    # Pasivos
    ("2", "PASIVO", "pasivo", "acreedora", None, 1),
    ("21", "Pasivo Corriente", "pasivo", "acreedora", "2", 2),
    ("2101", "Cuentas por Pagar Comerciales", "pasivo", "acreedora", "21", 3),
    ("210101", "Proveedores", "pasivo", "acreedora", "2101", 4),
    ("2102", "IVA Debito Fiscal", "pasivo", "acreedora", "21", 3),
    ("2103", "Retenciones por Pagar", "pasivo", "acreedora", "21", 3),
    # Patrimonio
    ("3", "PATRIMONIO", "patrimonio", "acreedora", None, 1),
    ("31", "Capital Social", "patrimonio", "acreedora", "3", 2),
    ("3101", "Capital Suscrito", "patrimonio", "acreedora", "31", 3),
    ("32", "Resultados", "patrimonio", "acreedora", "3", 2),
    ("3201", "Utilidad del Ejercicio", "patrimonio", "acreedora", "32", 3),
    # Ingresos
    ("4", "INGRESOS", "ingreso", "acreedora", None, 1),
    ("41", "Ingresos por Ventas", "ingreso", "acreedora", "4", 2),
    ("4101", "Ventas Gravadas", "ingreso", "acreedora", "41", 3),
    ("4102", "Ventas Exentas", "ingreso", "acreedora", "41", 3),
    ("4103", "Ventas No Sujetas", "ingreso", "acreedora", "41", 3),
    ("4104", "Exportaciones", "ingreso", "acreedora", "41", 3),
    # Costos
    ("5", "COSTOS", "costo", "deudora", None, 1),
    ("51", "Costo de Ventas", "costo", "deudora", "5", 2),
    ("5101", "Costo de Mercaderias Vendidas", "costo", "deudora", "51", 3),
    # Gastos
    ("6", "GASTOS", "gasto", "deudora", None, 1),
    ("61", "Gastos de Operacion", "gasto", "deudora", "6", 2),
    ("6101", "Gastos de Venta", "gasto", "deudora", "61", 3),
    ("6102", "Gastos de Administracion", "gasto", "deudora", "61", 3),
]


async def seed_default_accounts(supabase: Any, org_id: str) -> dict:
    """Seed default chart of accounts for a new org."""
    existing = supabase.table("chart_of_accounts").select(
        "id", count="exact"
    ).eq("org_id", org_id).execute()

    if (existing.count or len(existing.data or [])) > 0:
        return {"seeded": False, "message": "Ya existen cuentas configuradas", "count": 0}

    # First pass: insert accounts without padre references
    code_to_id = {}
    for codigo, nombre, tipo, naturaleza, padre_codigo, nivel in DEFAULT_ACCOUNTS:
        record = {
            "org_id": org_id,
            "codigo": codigo,
            "nombre": nombre,
            "tipo": tipo,
            "naturaleza": naturaleza,
            "cuenta_padre_id": None,
            "nivel": nivel,
            "es_detalle": nivel >= 4,
            "activa": True,
        }
        result = supabase.table("chart_of_accounts").insert(record).execute()
        if result.data:
            code_to_id[codigo] = result.data[0]["id"]

    # Second pass: update padre references
    for codigo, _, _, _, padre_codigo, _ in DEFAULT_ACCOUNTS:
        if padre_codigo and padre_codigo in code_to_id and codigo in code_to_id:
            supabase.table("chart_of_accounts").update({
                "cuenta_padre_id": code_to_id[padre_codigo]
            }).eq("id", code_to_id[codigo]).execute()

    return {"seeded": True, "count": len(code_to_id)}


async def list_accounts(
    supabase: Any, org_id: str, tipo: Optional[str] = None, solo_detalle: bool = False,
) -> list:
    """List chart of accounts."""
    query = supabase.table("chart_of_accounts").select("*").eq(
        "org_id", org_id
    ).eq("activa", True).order("codigo")

    if tipo:
        query = query.eq("tipo", tipo)
    if solo_detalle:
        query = query.eq("es_detalle", True)

    result = query.execute()
    return result.data or []


async def create_account(supabase: Any, org_id: str, data: dict) -> dict:
    """Create a custom account."""
    # Check unique code
    existing = supabase.table("chart_of_accounts").select("id").eq(
        "org_id", org_id
    ).eq("codigo", data["codigo"]).execute()
    if existing.data:
        raise ValueError(f"Ya existe una cuenta con codigo {data['codigo']}")

    record = {
        "org_id": org_id,
        "codigo": data["codigo"],
        "nombre": data["nombre"],
        "tipo": data["tipo"],
        "naturaleza": data["naturaleza"],
        "cuenta_padre_id": data.get("cuenta_padre_id"),
        "nivel": data.get("nivel", 4),
        "es_detalle": data.get("es_detalle", True),
        "activa": True,
    }
    result = supabase.table("chart_of_accounts").insert(record).execute()
    return result.data[0] if result.data else record


async def list_journal_entries(
    supabase: Any, org_id: str,
    fecha_from: Optional[str] = None, fecha_to: Optional[str] = None,
    tipo: Optional[str] = None,
    page: int = 1, per_page: int = 30,
) -> dict:
    """List journal entries with filters."""
    query = supabase.table("journal_entries").select(
        "*, journal_entry_lines(*)", count="exact"
    ).eq("org_id", org_id).order("fecha", desc=True).order("numero", desc=True)

    if fecha_from:
        query = query.gte("fecha", fecha_from)
    if fecha_to:
        query = query.lte("fecha", fecha_to)
    if tipo:
        query = query.eq("tipo", tipo)

    offset = (page - 1) * per_page
    query = query.range(offset, offset + per_page - 1)
    result = query.execute()

    return {
        "data": result.data or [],
        "total": result.count or 0,
        "page": page,
        "per_page": per_page,
    }


async def _get_next_entry_number(supabase: Any, org_id: str) -> int:
    """Get next journal entry number."""
    result = supabase.table("journal_entries").select("numero").eq(
        "org_id", org_id
    ).order("numero", desc=True).limit(1).execute()

    if result.data:
        return result.data[0]["numero"] + 1
    return 1


async def create_manual_entry(
    supabase: Any, org_id: str, user_id: str, data: dict,
) -> dict:
    """Create a manual journal entry."""
    lines = data.get("lineas", [])
    if len(lines) < 2:
        raise ValueError("Una partida debe tener al menos 2 lineas")

    total_debe = sum(float(l.get("debe", 0)) for l in lines)
    total_haber = sum(float(l.get("haber", 0)) for l in lines)

    if abs(total_debe - total_haber) > 0.01:
        raise ValueError(
            f"La partida no cuadra. Debe: ${total_debe:.2f}, Haber: ${total_haber:.2f}"
        )

    numero = await _get_next_entry_number(supabase, org_id)

    entry = {
        "org_id": org_id,
        "numero": numero,
        "fecha": data.get("fecha", date.today().isoformat()),
        "descripcion": data["descripcion"],
        "tipo": "manual",
        "total_debe": total_debe,
        "total_haber": total_haber,
        "estado": "registrada",
        "created_by": user_id,
    }
    entry_result = supabase.table("journal_entries").insert(entry).execute()
    if not entry_result.data:
        raise ValueError("Error al crear partida")

    entry_id = entry_result.data[0]["id"]

    for line in lines:
        supabase.table("journal_entry_lines").insert({
            "journal_entry_id": entry_id,
            "org_id": org_id,
            "cuenta_id": line["cuenta_id"],
            "cuenta_codigo": line.get("cuenta_codigo", ""),
            "cuenta_nombre": line.get("cuenta_nombre", ""),
            "debe": float(line.get("debe", 0)),
            "haber": float(line.get("haber", 0)),
            "concepto": line.get("concepto", ""),
        }).execute()

    return entry_result.data[0]


async def generate_dte_entry(
    supabase: Any, org_id: str, user_id: str,
    tipo_dte: str, numero_control: str, codigo_gen: str,
    receptor_nombre: str, monto_total: float,
    total_gravada: float, total_exenta: float, total_no_suj: float,
    iva: float, condicion: int,
) -> None:
    """
    Auto-generate journal entry from DTE emission.
    Non-blocking — called from dte_service.py post-emission.
    """
    try:
        # Get detail accounts for this org
        accounts = supabase.table("chart_of_accounts").select(
            "id, codigo, nombre"
        ).eq("org_id", org_id).eq("es_detalle", True).eq("activa", True).execute()

        accts = {a["codigo"]: a for a in (accounts.data or [])}

        # Need at minimum: Caja/CxC (debit) + Ventas (credit) + IVA DF (credit)
        # Contado (1) → debit Caja; Credito (2) → debit CxC
        debit_code = "110101" if condicion == 1 else "110201"
        debit_acct = accts.get(debit_code)
        ventas_acct = accts.get("4101")  # Ventas Gravadas
        ventas_ex_acct = accts.get("4102")  # Ventas Exentas
        iva_acct = accts.get("2102")  # IVA Debito Fiscal

        if not debit_acct or not ventas_acct:
            logger.warning(f"Missing accounts for org {org_id}, skipping auto-entry")
            return

        numero = await _get_next_entry_number(supabase, org_id)

        tipo_nombre = {
            "01": "Factura", "03": "CCF", "05": "Nota Credito", "06": "Nota Debito",
            "11": "Sujeto Excluido", "14": "Exportacion",
        }.get(tipo_dte, tipo_dte)

        desc = f"{tipo_nombre} {numero_control} — {receptor_nombre}"

        entry = {
            "org_id": org_id,
            "numero": numero,
            "fecha": date.today().isoformat(),
            "descripcion": desc,
            "tipo": "automatica",
            "referencia_tipo": "dte",
            "referencia_id": codigo_gen,
            "total_debe": monto_total,
            "total_haber": monto_total,
            "estado": "registrada",
            "created_by": user_id,
        }
        entry_result = supabase.table("journal_entries").insert(entry).execute()
        if not entry_result.data:
            return

        entry_id = entry_result.data[0]["id"]
        lines = []

        # Debit: Caja or CxC for total
        lines.append({
            "journal_entry_id": entry_id, "org_id": org_id,
            "cuenta_id": debit_acct["id"], "cuenta_codigo": debit_acct["codigo"],
            "cuenta_nombre": debit_acct["nombre"],
            "debe": monto_total, "haber": 0,
            "concepto": desc,
        })

        # Credit: Ventas Gravadas (neto sin IVA para tipo 01, o gravada para 03)
        venta_neta = total_gravada
        if tipo_dte == "01" and iva > 0:
            venta_neta = total_gravada - iva

        if venta_neta > 0:
            lines.append({
                "journal_entry_id": entry_id, "org_id": org_id,
                "cuenta_id": ventas_acct["id"], "cuenta_codigo": ventas_acct["codigo"],
                "cuenta_nombre": ventas_acct["nombre"],
                "debe": 0, "haber": round(venta_neta, 2),
                "concepto": f"Venta gravada {tipo_nombre}",
            })

        # Credit: Ventas Exentas
        if total_exenta > 0 and ventas_ex_acct:
            lines.append({
                "journal_entry_id": entry_id, "org_id": org_id,
                "cuenta_id": ventas_ex_acct["id"], "cuenta_codigo": ventas_ex_acct["codigo"],
                "cuenta_nombre": ventas_ex_acct["nombre"],
                "debe": 0, "haber": total_exenta,
                "concepto": f"Venta exenta {tipo_nombre}",
            })

        # Credit: IVA Debito Fiscal
        if iva > 0 and iva_acct:
            lines.append({
                "journal_entry_id": entry_id, "org_id": org_id,
                "cuenta_id": iva_acct["id"], "cuenta_codigo": iva_acct["codigo"],
                "cuenta_nombre": iva_acct["nombre"],
                "debe": 0, "haber": round(iva, 2),
                "concepto": f"IVA DF {tipo_nombre}",
            })

        for line in lines:
            supabase.table("journal_entry_lines").insert(line).execute()

        logger.info(f"Auto-entry #{numero} for DTE {codigo_gen[:8]}")

    except Exception as e:
        logger.error(f"Auto journal entry error: {e}")


async def get_balance_general(supabase: Any, org_id: str, fecha_corte: Optional[str] = None) -> dict:
    """Simple trial balance (balance de comprobacion)."""
    query = supabase.table("journal_entry_lines").select(
        "cuenta_id, cuenta_codigo, cuenta_nombre, debe, haber"
    ).eq("org_id", org_id)

    if fecha_corte:
        # Filter by entry date — need to join, but supabase limits joins
        # So we get all entries up to date, then filter lines
        entries = supabase.table("journal_entries").select("id").eq(
            "org_id", org_id
        ).eq("estado", "registrada").lte("fecha", fecha_corte).execute()
        entry_ids = [e["id"] for e in (entries.data or [])]
        if not entry_ids:
            return {"cuentas": [], "total_debe": 0, "total_haber": 0}
        query = query.in_("journal_entry_id", entry_ids)

    result = query.execute()
    rows = result.data or []

    # Aggregate by account
    saldos = {}
    for r in rows:
        cid = r["cuenta_id"]
        if cid not in saldos:
            saldos[cid] = {
                "cuenta_id": cid,
                "codigo": r["cuenta_codigo"],
                "nombre": r["cuenta_nombre"],
                "total_debe": 0,
                "total_haber": 0,
            }
        saldos[cid]["total_debe"] += float(r.get("debe", 0))
        saldos[cid]["total_haber"] += float(r.get("haber", 0))

    cuentas = sorted(saldos.values(), key=lambda x: x["codigo"])
    for c in cuentas:
        c["saldo"] = round(c["total_debe"] - c["total_haber"], 2)
        c["total_debe"] = round(c["total_debe"], 2)
        c["total_haber"] = round(c["total_haber"], 2)

    return {
        "cuentas": cuentas,
        "total_debe": round(sum(c["total_debe"] for c in cuentas), 2),
        "total_haber": round(sum(c["total_haber"] for c in cuentas), 2),
    }
