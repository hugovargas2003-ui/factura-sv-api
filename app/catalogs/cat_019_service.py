"""
CAT-019 Activity Code Catalog Service
Loads the official MH CAT-019 JSON and provides search/autocomplete functionality.

Place this file at: app/catalogs/cat_019_service.py
Place the CAT-019_MH_ES.json at: app/catalogs/CAT-019_MH_ES.json
"""

import json
import os
from pathlib import Path
from functools import lru_cache
from typing import Optional

# ─── Data Structure ───────────────────────────────────────────────
# Each flattened record:
# {
#   "codigo": "58200",
#   "descripcion": "Edición de programas informáticos (software)",
#   "seccion": "J",
#   "division": "58",
#   "grupo": "582",
#   "clase": "5820",
#   "search_text": "j 58 582 5820 58200 edicion de programas informaticos software ..."
# }

_CATALOG: list[dict] = []
_LOADED = False


def _normalize(text: str) -> str:
    """Remove accents and lowercase for search matching."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _flatten_catalog(data: dict) -> list[dict]:
    """Flatten hierarchical CAT-019 JSON into searchable subclass records."""
    records = []
    
    for seccion in data.get("secciones", []):
        sec_code = seccion.get("codigo", "")
        sec_desc = seccion.get("descripcion", "")
        
        for division in seccion.get("divisiones", []):
            div_code = division.get("codigo", "")
            div_desc = division.get("descripcion", "")
            
            for grupo in division.get("grupos", []):
                grp_code = grupo.get("codigo", "")
                grp_desc = grupo.get("descripcion", "")
                
                for clase in grupo.get("clases", []):
                    cls_code = clase.get("codigo", "")
                    cls_desc = clase.get("descripcion", "")
                    
                    for subclase in clase.get("subclases", []):
                        sub_code = subclase.get("codigo", "")
                        sub_desc = subclase.get("descripcion", "")
                        
                        # Skip disabled codes
                        if "inhabilitado" in sub_desc.lower():
                            continue
                        
                        # Build composite search text for fuzzy matching
                        search_parts = [
                            sec_code, sec_desc,
                            div_code, div_desc,
                            grp_code, grp_desc,
                            cls_code, cls_desc,
                            sub_code, sub_desc,
                        ]
                        search_text = _normalize(" ".join(search_parts))
                        
                        records.append({
                            "codigo": sub_code,
                            "descripcion": sub_desc,
                            "seccion": sec_code,
                            "seccion_desc": sec_desc,
                            "division": div_code,
                            "grupo": grp_code,
                            "clase": cls_code,
                            "search_text": search_text,
                        })
    
    return records


def _load():
    """Load and flatten the catalog from JSON file."""
    global _CATALOG, _LOADED
    if _LOADED:
        return
    
    # Look for JSON file relative to this module
    catalog_path = Path(__file__).parent / "CAT-019_MH_ES.json"
    
    if not catalog_path.exists():
        # Fallback: check project root
        catalog_path = Path(__file__).parent.parent / "CAT-019_MH_ES.json"
    
    if not catalog_path.exists():
        raise FileNotFoundError(
            f"CAT-019_MH_ES.json not found. Expected at {catalog_path}"
        )
    
    with open(catalog_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    _CATALOG = _flatten_catalog(data)
    _LOADED = True
    print(f"[CAT-019] Loaded {len(_CATALOG)} activity codes")


def search_activities(query: str, limit: int = 20) -> list[dict]:
    """
    Search activity codes by keyword or code prefix.
    
    Examples:
        search_activities("software")  → codes related to software
        search_activities("582")       → codes starting with 582
        search_activities("restaurante") → restaurant-related codes
    
    Returns list of {codigo, descripcion, seccion, division, grupo, clase}
    """
    _load()
    
    if not query or not query.strip():
        return []
    
    q = _normalize(query.strip())
    terms = q.split()
    
    results = []
    for record in _CATALOG:
        # Check if ALL terms match somewhere in the search text
        if all(term in record["search_text"] for term in terms):
            results.append({
                "codigo": record["codigo"],
                "descripcion": record["descripcion"],
                "seccion": record["seccion"],
                "division": record["division"],
                "grupo": record["grupo"],
                "clase": record["clase"],
            })
        
        if len(results) >= limit:
            break
    
    # Sort: exact code prefix matches first, then alphabetical
    results.sort(key=lambda r: (
        0 if r["codigo"].startswith(q) else 1,
        r["codigo"]
    ))
    
    return results


def get_activity(codigo: str) -> Optional[dict]:
    """Get a single activity by exact code. Returns None if not found."""
    _load()
    for record in _CATALOG:
        if record["codigo"] == codigo:
            return {
                "codigo": record["codigo"],
                "descripcion": record["descripcion"],
                "seccion": record["seccion"],
                "division": record["division"],
                "grupo": record["grupo"],
                "clase": record["clase"],
            }
    return None


def validate_activity(codigo: str, descripcion: str) -> tuple[bool, str]:
    """
    Validate that codigo+descripcion match the CAT-019 catalog.
    Returns (is_valid, error_message).
    """
    _load()
    
    record = get_activity(codigo)
    if not record:
        return False, f"Código de actividad '{codigo}' no existe en el catálogo CAT-019"
    
    if record["descripcion"] != descripcion:
        return False, (
            f"La descripción no coincide con el catálogo CAT-019. "
            f"Para código '{codigo}' se espera: '{record['descripcion']}'"
        )
    
    return True, ""


def get_all_count() -> int:
    """Return total number of activity codes in catalog."""
    _load()
    return len(_CATALOG)
