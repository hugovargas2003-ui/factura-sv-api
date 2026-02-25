"""
inventory_service.py — Inventory management and Kardex.

Location: app/services/inventory_service.py
NEW FILE — does not modify any existing infrastructure.

Features:
- Register inventory movements (entrada, salida, ajuste)
- Auto-deduct stock on DTE emission
- Kardex report per product
- Stock alerts (below minimum)
- Weighted average cost (NIIF PYMES 13.18)
"""

from typing import Any, Optional
from datetime import date


# ---------------------------------------------------------------------------
# Stock movements
# ---------------------------------------------------------------------------

async def register_movement(
    supabase: Any,
    org_id: str,
    producto_id: str,
    tipo: str,
    cantidad: float,
    costo_unitario: float = 0,
    referencia: str = "",
    created_by: str = "",
) -> dict:
    """
    Register an inventory movement and update stock.
    tipo: entrada, salida, ajuste, dte_emitido
    """
    if tipo not in ("entrada", "salida", "ajuste", "dte_emitido"):
        raise ValueError(f"Tipo invalido: {tipo}")
    if cantidad <= 0:
        raise ValueError("Cantidad debe ser mayor a 0")

    # Get current product
    prod = supabase.table("dte_productos").select(
        "id, descripcion, stock_actual, costo_promedio, track_inventory"
    ).eq("id", producto_id).eq("org_id", org_id).single().execute()

    if not prod.data:
        raise ValueError("Producto no encontrado")

    stock_anterior = float(prod.data["stock_actual"] or 0)
    costo_prom_anterior = float(prod.data["costo_promedio"] or 0)

    # Calculate new stock
    if tipo in ("entrada", "ajuste"):
        stock_posterior = stock_anterior + cantidad
        # Weighted average cost (NIIF PYMES 13.18)
        if tipo == "entrada" and costo_unitario > 0:
            total_anterior = stock_anterior * costo_prom_anterior
            total_nuevo = cantidad * costo_unitario
            nuevo_costo = (
                (total_anterior + total_nuevo) / stock_posterior
                if stock_posterior > 0 else costo_unitario
            )
        else:
            nuevo_costo = costo_prom_anterior
    else:
        # salida or dte_emitido
        stock_posterior = stock_anterior - cantidad
        nuevo_costo = costo_prom_anterior

    # Insert movement
    supabase.table("inventory_movements").insert({
        "org_id": org_id,
        "producto_id": producto_id,
        "tipo": tipo,
        "cantidad": cantidad,
        "costo_unitario": costo_unitario or costo_prom_anterior,
        "referencia": referencia,
        "stock_anterior": stock_anterior,
        "stock_posterior": stock_posterior,
        "created_by": created_by,
    }).execute()

    # Update product stock
    supabase.table("dte_productos").update({
        "stock_actual": stock_posterior,
        "costo_promedio": round(nuevo_costo, 4),
    }).eq("id", producto_id).execute()

    return {
        "success": True,
        "producto_id": producto_id,
        "tipo": tipo,
        "cantidad": cantidad,
        "stock_anterior": stock_anterior,
        "stock_posterior": stock_posterior,
        "costo_promedio": round(nuevo_costo, 4),
    }


# ---------------------------------------------------------------------------
# Auto-deduct on DTE emission
# ---------------------------------------------------------------------------

async def deduct_stock_for_dte(
    supabase: Any,
    org_id: str,
    items: list[dict],
    numero_control: str,
    user_id: str = "",
) -> list[dict]:
    """
    Deduct stock for each item that has track_inventory=true.
    Called after successful DTE emission.
    items: list of {codigo, cantidad} from emission.
    """
    results = []
    for item in items:
        codigo = item.get("codigo")
        cantidad = float(item.get("cantidad", 0))
        if not codigo or cantidad <= 0:
            continue

        # Find product by codigo
        prod = supabase.table("dte_productos").select(
            "id, track_inventory, stock_actual"
        ).eq("org_id", org_id).eq("codigo", codigo).execute()

        if not prod.data:
            continue

        p = prod.data[0]
        if not p.get("track_inventory"):
            continue

        try:
            r = await register_movement(
                supabase, org_id, p["id"],
                tipo="dte_emitido",
                cantidad=cantidad,
                referencia=f"DTE {numero_control}",
                created_by=user_id,
            )
            results.append(r)
        except Exception as e:
            results.append({"producto_id": p["id"], "error": str(e)})

    return results


# ---------------------------------------------------------------------------
# Kardex report
# ---------------------------------------------------------------------------

async def get_kardex(
    supabase: Any,
    org_id: str,
    producto_id: str,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> dict:
    """Kardex: all movements for a product with running balance."""
    # Product info
    prod = supabase.table("dte_productos").select(
        "id, codigo, descripcion, stock_actual, costo_promedio, stock_minimo"
    ).eq("id", producto_id).eq("org_id", org_id).single().execute()

    if not prod.data:
        raise ValueError("Producto no encontrado")

    query = supabase.table("inventory_movements").select(
        "id, tipo, cantidad, costo_unitario, referencia, "
        "stock_anterior, stock_posterior, created_at"
    ).eq("producto_id", producto_id).order("created_at")

    if fecha_desde:
        query = query.gte("created_at", fecha_desde)
    if fecha_hasta:
        query = query.lte("created_at", fecha_hasta + "T23:59:59")

    result = query.execute()
    movements = result.data or []

    # Calculate valorized balance
    for m in movements:
        cu = float(m.get("costo_unitario", 0) or 0)
        qty = float(m.get("cantidad", 0))
        m["valor_movimiento"] = round(cu * qty, 2)
        sp = float(m.get("stock_posterior", 0) or 0)
        m["valor_stock"] = round(sp * cu, 2)

    return {
        "producto": prod.data,
        "movements": movements,
        "total_movements": len(movements),
    }


# ---------------------------------------------------------------------------
# Stock overview and alerts
# ---------------------------------------------------------------------------

async def get_stock_overview(
    supabase: Any, org_id: str, alerts_only: bool = False
) -> dict:
    """List all products with inventory tracking and current stock."""
    query = supabase.table("dte_productos").select(
        "id, codigo, descripcion, stock_actual, stock_minimo, "
        "costo_promedio, track_inventory, precio_unitario"
    ).eq("org_id", org_id).eq("track_inventory", True).order("descripcion")

    result = query.execute()
    productos = result.data or []

    alertas = []
    total_valor = 0.0

    for p in productos:
        stock = float(p.get("stock_actual", 0) or 0)
        minimo = float(p.get("stock_minimo", 0) or 0)
        costo = float(p.get("costo_promedio", 0) or 0)
        valor = stock * costo
        total_valor += valor
        p["valor_inventario"] = round(valor, 2)

        if minimo > 0 and stock <= minimo:
            alertas.append({
                "producto_id": p["id"],
                "codigo": p.get("codigo", ""),
                "descripcion": p["descripcion"],
                "stock_actual": stock,
                "stock_minimo": minimo,
                "deficit": round(minimo - stock, 2),
            })

    if alerts_only:
        return {"alertas": alertas, "total_alertas": len(alertas)}

    return {
        "productos": productos,
        "total_productos": len(productos),
        "total_valor_inventario": round(total_valor, 2),
        "alertas": alertas,
        "total_alertas": len(alertas),
    }
