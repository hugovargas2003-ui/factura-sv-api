"""
contador_service.py — Multi-client dashboard for accounting firms.

Location: app/services/contador_service.py
NEW FILE — does not modify any existing infrastructure.

Handles:
- Consolidated dashboard across all user's client orgs
- Cross-org reporting
- Add new client org and auto-link
"""

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


async def get_contador_dashboard(supabase: Any, user_id: str) -> dict:
    """
    Consolidated dashboard: stats for ALL orgs the user belongs to.
    Single query with org_id IN (...), aggregation in Python.
    """
    # 1. Get all user memberships
    memberships = supabase.table("user_organizations").select(
        "org_id, role"
    ).eq("user_id", user_id).execute()

    if not memberships.data or len(memberships.data) == 0:
        return {"firms": [], "totals": _empty_totals(), "alertas": []}

    org_ids = [m["org_id"] for m in memberships.data]
    role_map = {m["org_id"]: m["role"] for m in memberships.data}

    # 2. Get org details
    orgs = supabase.table("organizations").select(
        "id, name, nit, plan, monthly_quota"
    ).in_("id", org_ids).execute()
    org_map = {o["id"]: o for o in (orgs.data or [])}

    # 3. Current month boundaries
    now = datetime.now(timezone.utc)
    primer_dia = now.strftime("%Y-%m-01")

    # 4. Single query: all DTEs for all orgs this month
    dtes = supabase.table("dtes").select(
        "org_id, monto_total, estado, tipo_dte, fecha_emision"
    ).in_("org_id", org_ids).gte(
        "fecha_emision", primer_dia
    ).execute()

    # 5. Aggregate by org
    org_stats: dict[str, dict] = {oid: {
        "total_dtes": 0, "dtes_procesados": 0,
        "monto_total": 0.0, "por_tipo": {},
    } for oid in org_ids}

    for d in (dtes.data or []):
        oid = d["org_id"]
        if oid not in org_stats:
            continue
        s = org_stats[oid]
        s["total_dtes"] += 1
        if d.get("estado") == "procesado":
            s["dtes_procesados"] += 1
            s["monto_total"] += float(d.get("monto_total") or 0)
        tipo = d.get("tipo_dte", "??")
        s["por_tipo"][tipo] = s["por_tipo"].get(tipo, 0) + 1

    # 6. CxC pendientes — single query across all orgs
    cxc_data = supabase.table("dtes").select(
        "org_id, monto_total, monto_pagado"
    ).in_("org_id", org_ids).eq(
        "estado", "procesado"
    ).eq("estado_pago", "pendiente").execute()

    cxc_by_org: dict[str, float] = {}
    for c in (cxc_data.data or []):
        oid = c["org_id"]
        total = float(c.get("monto_total") or 0)
        pagado = float(c.get("monto_pagado") or 0)
        cxc_by_org[oid] = cxc_by_org.get(oid, 0.0) + (total - pagado)

    # 7. Build firms array
    firms = []
    alertas = []
    total_dtes_all = 0
    total_monto_all = 0.0
    total_pendiente_all = 0.0

    for oid in org_ids:
        org = org_map.get(oid, {})
        stats = org_stats.get(oid, {})
        cuota = org.get("monthly_quota") or 0
        pendiente = cxc_by_org.get(oid, 0.0)

        firm = {
            "org_id": oid,
            "org_name": org.get("name", "—"),
            "nit": org.get("nit", ""),
            "plan": org.get("plan", "free"),
            "role": role_map.get(oid, "member"),
            "dtes_mes": stats.get("dtes_procesados", 0),
            "monto_mes": round(stats.get("monto_total", 0), 2),
            "pendiente_cobro": round(pendiente, 2),
            "cuota_used": stats.get("total_dtes", 0),
            "cuota_limit": cuota,
            "por_tipo": stats.get("por_tipo", {}),
        }
        firms.append(firm)

        total_dtes_all += firm["dtes_mes"]
        total_monto_all += firm["monto_mes"]
        total_pendiente_all += firm["pendiente_cobro"]

        # Alertas
        if cuota > 0 and stats.get("total_dtes", 0) >= cuota * 0.8:
            pct = round(stats["total_dtes"] / cuota * 100)
            alertas.append({
                "org_name": firm["org_name"],
                "tipo": "cuota_baja",
                "mensaje": f"Cuota al {pct}% ({stats['total_dtes']}/{cuota})",
            })
        if pendiente > 500:
            alertas.append({
                "org_name": firm["org_name"],
                "tipo": "pago_pendiente",
                "mensaje": f"${pendiente:,.2f} pendiente de cobro",
            })

    firms.sort(key=lambda f: f["monto_mes"], reverse=True)

    return {
        "firms": firms,
        "totals": {
            "total_orgs": len(org_ids),
            "total_dtes_mes": total_dtes_all,
            "total_monto_mes": round(total_monto_all, 2),
            "total_pendiente_cobro": round(total_pendiente_all, 2),
        },
        "alertas": alertas,
    }


async def get_cross_org_report(
    supabase: Any, user_id: str,
    fecha_desde: str, fecha_hasta: str,
) -> dict:
    """
    Consolidated report across all user's orgs for a date range.
    Returns breakdown by org and DTE type.
    """
    memberships = supabase.table("user_organizations").select(
        "org_id"
    ).eq("user_id", user_id).execute()

    if not memberships.data:
        return {"by_org": [], "grand_total": {}}

    org_ids = [m["org_id"] for m in memberships.data]

    orgs = supabase.table("organizations").select(
        "id, name"
    ).in_("id", org_ids).execute()
    org_map = {o["id"]: o["name"] for o in (orgs.data or [])}

    dtes = supabase.table("dtes").select(
        "org_id, tipo_dte, monto_total, total_gravada, iva, estado"
    ).in_("org_id", org_ids).gte(
        "fecha_emision", fecha_desde
    ).lte(
        "fecha_emision", fecha_hasta
    ).eq("estado", "procesado").execute()

    # Aggregate by org
    by_org: dict[str, dict] = {}
    for d in (dtes.data or []):
        oid = d["org_id"]
        if oid not in by_org:
            by_org[oid] = {
                "org_id": oid,
                "org_name": org_map.get(oid, "—"),
                "facturas": 0, "ccf": 0, "nc": 0, "nd": 0,
                "fse": 0, "fex": 0, "otros": 0,
                "total_gravada": 0.0, "total_iva": 0.0,
                "total": 0.0,
            }
        b = by_org[oid]
        tipo = d.get("tipo_dte", "")
        tipo_key = {
            "01": "facturas", "03": "ccf", "05": "nc",
            "06": "nd", "11": "fse", "14": "fex",
        }.get(tipo, "otros")
        b[tipo_key] += 1
        b["total_gravada"] += float(d.get("total_gravada") or 0)
        b["total_iva"] += float(d.get("iva") or 0)
        b["total"] += float(d.get("monto_total") or 0)

    # Round
    result_list = []
    grand = {"facturas": 0, "ccf": 0, "nc": 0, "nd": 0, "fse": 0,
             "fex": 0, "otros": 0, "total_gravada": 0.0,
             "total_iva": 0.0, "total": 0.0}

    for b in by_org.values():
        b["total_gravada"] = round(b["total_gravada"], 2)
        b["total_iva"] = round(b["total_iva"], 2)
        b["total"] = round(b["total"], 2)
        result_list.append(b)
        for k in grand:
            grand[k] += b[k]

    grand["total_gravada"] = round(grand["total_gravada"], 2)
    grand["total_iva"] = round(grand["total_iva"], 2)
    grand["total"] = round(grand["total"], 2)

    result_list.sort(key=lambda x: x["total"], reverse=True)
    return {"by_org": result_list, "grand_total": grand}


async def add_client_org(
    supabase: Any, contador_user_id: str,
    contador_org_id: str,
    data: dict,
) -> dict:
    """
    Create a new client org and auto-link the contador as owner.
    Enforces plan limits on number of organizations.
    """
    # 1. Plan enforcement — count current orgs
    memberships = supabase.table("user_organizations").select(
        "id"
    ).eq("user_id", contador_user_id).execute()
    current_count = len(memberships.data or [])

    # Get contador's own org plan
    contador_org = supabase.table("organizations").select(
        "plan"
    ).eq("id", contador_org_id).single().execute()
    plan = (contador_org.data or {}).get("plan", "free")

    # Plan limits
    plan_limits = {
        "free": 1, "micro": 1, "basico": 2,
        "profesional": 3, "contador": 53,
        "empresarial": 10, "enterprise": 100,
    }
    max_orgs = plan_limits.get(plan, 2)

    if current_count >= max_orgs:
        raise ValueError(
            f"Tu plan '{plan}' permite {max_orgs} empresas. "
            f"Ya tienes {current_count}. Actualiza tu plan para agregar más."
        )

    # 2. Create new organization
    org_result = supabase.table("organizations").insert({
        "name": data["nombre"],
        "nit": data.get("nit", ""),
        "nrc": data.get("nrc", ""),
        "plan": "free",
        "monthly_quota": 50,
    }).execute()

    if not org_result.data:
        raise ValueError("Error creando la organización")

    new_org = org_result.data[0]
    new_org_id = new_org["id"]

    # 3. Link contador as owner
    supabase.table("user_organizations").insert({
        "user_id": contador_user_id,
        "org_id": new_org_id,
        "role": "owner",
        "is_default": False,
    }).execute()

    # 4. Create dte_credentials placeholder
    supabase.table("dte_credentials").upsert({
        "org_id": new_org_id,
        "nit": data.get("nit", ""),
        "nrc": data.get("nrc", ""),
        "nombre": data["nombre"],
        "cod_actividad": data.get("cod_actividad", ""),
    }, on_conflict="org_id").execute()

    logger.info(
        f"Contador {contador_user_id} created client org {new_org_id}: {data['nombre']}"
    )

    return {
        "success": True,
        "org_id": new_org_id,
        "name": data["nombre"],
        "message": "Empresa creada. Configure las credenciales MH desde Configuración.",
    }


def _empty_totals() -> dict:
    return {
        "total_orgs": 0, "total_dtes_mes": 0,
        "total_monto_mes": 0, "total_pendiente_cobro": 0,
    }
