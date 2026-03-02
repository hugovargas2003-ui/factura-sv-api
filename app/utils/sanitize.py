"""
NRC Sanitization Patch

The MH requires NRC in specific format:
- Only digits (no hyphens, no spaces)
- No leading zeros (unless the NRC actually starts with zero)
- Example valid: "1549809", "234567"
- Example invalid: "154-9809", "01549809", "1549809-0"

Apply this patch in app/mh/dte_builder.py

Add this function at the top of the file, then call it wherever
receptor.get("nrc") or e["nrc"] is used.
"""


def sanitize_nrc(nrc: str | None) -> str | None:
    """
    Sanitize NRC for MH compliance.
    - Strips hyphens, spaces, dots
    - Removes leading zeros
    - Returns None if empty/invalid
    """
    if not nrc:
        return None
    
    # Remove common separators
    cleaned = nrc.replace("-", "").replace(" ", "").replace(".", "").strip()
    
    # Remove leading zeros (NRC "01234" -> "1234")
    cleaned = cleaned.lstrip("0") or "0"
    
    # Must be numeric
    if not cleaned.isdigit():
        return None
    
    return cleaned


# ─── HOW TO APPLY ─────────────────────────────────────────────────
#
# In dte_builder.py, at the top:
#
#   from app.utils.sanitize import sanitize_nrc
#
# Then in EVERY place where NRC is set in the DTE JSON:
#
#   BEFORE:  "nrc": receptor.get("nrc"),
#   AFTER:   "nrc": sanitize_nrc(receptor.get("nrc")),
#
#   BEFORE:  "nrc": e["nrc"],
#   AFTER:   "nrc": sanitize_nrc(e["nrc"]),
#
# Lines to patch (from grep output):
#   172, 237, 283, 297, 388, 500, 548, 560, 612, 630
#
# ─── QUICK SED COMMAND ───────────────────────────────────────────
#
# After adding the import, run:
#
# cd ~/Desktop/factura-sv-api
# sed -i 's/"nrc": receptor\.get("nrc")/"nrc": sanitize_nrc(receptor.get("nrc"))/g' app/mh/dte_builder.py
# sed -i 's/"nrc": e\["nrc"\]/"nrc": sanitize_nrc(e["nrc"])/g' app/mh/dte_builder.py
# sed -i 's/"nrc": r\.get("nrc")/"nrc": sanitize_nrc(r.get("nrc"))/g' app/mh/dte_builder.py
#
# ─────────────────────────────────────────────────────────────────
