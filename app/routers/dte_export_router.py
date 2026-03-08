"""
DTE Export Router — Export masivo de PDFs en ZIP.
Separado de dte_router.py para mantener modularidad.
"""
import io
import logging
import zipfile
import base64
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Response

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["dte-export"])

from app.dependencies import get_current_user, get_supabase


@router.get("/dtes/export-pdfs-zip")
async def export_pdfs_zip(
    fecha_desde: str = Query(..., description="YYYY-MM-DD"),
    fecha_hasta: str = Query(..., description="YYYY-MM-DD"),
    tipo_dte: str = Query(None, description="Filtrar por tipo DTE (01, 03, etc.)"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Download ZIP with all DTE PDFs from a period."""
    from app.services.pdf_generator import DTEPdfGenerator

    org_id = user.get("org_id")

    query = supabase.table("dtes") \
        .select("id, tipo_dte, numero_control, fecha_emision, receptor_nombre, documento_json, sello_recibido, estado") \
        .eq("org_id", org_id) \
        .in_("estado", ["procesado", "PROCESADO", "IMPORTADO"]) \
        .gte("fecha_emision", fecha_desde) \
        .lte("fecha_emision", fecha_hasta) \
        .order("fecha_emision")

    if tipo_dte:
        query = query.eq("tipo_dte", tipo_dte)

    dtes = query.limit(500).execute()

    if not dtes.data:
        raise HTTPException(status_code=404, detail="No hay DTEs en el periodo")

    # Fetch org logo once
    logo_bytes = None
    primary_color = None
    try:
        creds = supabase.table("mh_credentials").select(
            "logo_base64, primary_color"
        ).eq("org_id", org_id).single().execute()
        if creds.data:
            logo_b64 = creds.data.get("logo_base64")
            if logo_b64 and ";base64," in logo_b64:
                logo_bytes = base64.b64decode(logo_b64.split(";base64,")[1])
            pc = creds.data.get("primary_color")
            if pc and pc.startswith("#") and len(pc) == 7:
                primary_color = (int(pc[1:3], 16), int(pc[3:5], 16), int(pc[5:7], 16))
    except Exception:
        pass

    zip_buffer = io.BytesIO()
    generated = 0

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for dte in dtes.data:
            try:
                doc_json = dte.get("documento_json", {})
                if not doc_json:
                    continue

                generator = DTEPdfGenerator(
                    dte_json=doc_json,
                    sello=dte.get("sello_recibido"),
                    estado=dte.get("estado", "procesado"),
                    logo_bytes=logo_bytes,
                    primary_color=primary_color,
                )
                pdf_bytes = generator.generate()

                nc = dte.get("numero_control", dte["id"][:8])
                filename = f"{dte['fecha_emision']}_{dte['tipo_dte']}_{nc}.pdf"
                zf.writestr(filename, pdf_bytes)
                generated += 1

            except Exception as e:
                logger.warning(f"PDF generation failed for DTE {dte['id']}: {e}")
                continue

        # Summary file
        summary = f"Export PDFs — {fecha_desde} a {fecha_hasta}\n"
        summary += f"Total DTEs encontrados: {len(dtes.data)}\n"
        summary += f"PDFs generados: {generated}\n"
        summary += f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        zf.writestr("_RESUMEN.txt", summary)

    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=PDFs_{fecha_desde}_a_{fecha_hasta}.zip"},
    )
