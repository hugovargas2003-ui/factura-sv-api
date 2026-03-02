"""
Catalog Router - Provides searchable access to MH catalogs.

Place at: app/routers/catalog_router.py

Register in main.py:
    from app.routers.catalog_router import router as catalog_router
    app.include_router(catalog_router)
"""

from fastapi import APIRouter, Query, HTTPException
from app.catalogs.cat_019_service import search_activities, get_activity, get_all_count

router = APIRouter(prefix="/catalogo", tags=["catalogo"])


@router.get("/actividades")
async def search_actividades(
    q: str = Query("", description="Search keyword or code prefix"),
    limit: int = Query(20, ge=1, le=50),
):
    """Search CAT-019 economic activity codes for autocomplete."""
    results = search_activities(q, limit=limit)
    return {"query": q, "count": len(results), "total_catalog": get_all_count(), "results": results}


@router.get("/actividades/{codigo}")
async def get_actividad(codigo: str):
    """Get a single activity by exact code."""
    result = get_activity(codigo)
    if not result:
        raise HTTPException(status_code=404, detail=f"Codigo '{codigo}' no existe en CAT-019")
    return result
