"""
Contabilidad Export Router — XLSX profesional para Libro Diario y Estado de Resultados.
NO modifica endpoints existentes de contabilidad (esos están en dte_router.py).
"""
import io
import hashlib
import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/contabilidad", tags=["contabilidad-export"])

from app.dependencies import get_current_user, get_supabase, get_encryption
from app.services.contabilidad_service import list_journal_entries, get_balance_general
from app.services.contabilidad_export_service import generate_libro_diario_xlsx, generate_estado_resultados_xlsx


def _get_org_info(supabase, org_id: str) -> dict:
    """Get org name and NIT for report headers."""
    try:
        org = supabase.table("organizations").select("name, nit").eq("id", org_id).execute()
        if org.data:
            return {"name": org.data[0].get("name", ""), "nit": org.data[0].get("nit", "")}
    except Exception:
        pass
    return {"name": "", "nit": ""}


def _parse_periodo(periodo: str) -> tuple[str | None, str | None]:
    """Parse MMYYYY or YYYY into (fecha_from, fecha_to)."""
    if len(periodo) == 6:  # MMYYYY
        mes = int(periodo[:2])
        anio = int(periodo[2:])
        fecha_from = f"{anio}-{mes:02d}-01"
        if mes == 12:
            fecha_to = f"{anio + 1}-01-01"
        else:
            fecha_to = f"{anio}-{mes + 1:02d}-01"
        return fecha_from, fecha_to
    elif len(periodo) == 4:  # YYYY
        return f"{periodo}-01-01", f"{int(periodo) + 1}-01-01"
    return None, None


@router.get("/libro-diario/export")
async def export_libro_diario(
    periodo: str = Query(None, description="MMYYYY o YYYY — filtra por mes o año"),
    fecha_from: str = Query(None, description="Fecha inicio YYYY-MM-DD"),
    fecha_to: str = Query(None, description="Fecha fin YYYY-MM-DD"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Exportar Libro Diario en XLSX con formato contable profesional."""
    org_id = user.get("org_id")

    # Parse periodo to date range if provided
    if periodo and not fecha_from:
        fecha_from, fecha_to = _parse_periodo(periodo)

    # Get entries using existing service function
    result = await list_journal_entries(supabase, org_id, fecha_from=fecha_from, fecha_to=fecha_to, per_page=9999)
    entries = result.get("data", [])

    if not entries:
        raise HTTPException(status_code=404, detail="No hay partidas para el período seleccionado")

    org_info = _get_org_info(supabase, org_id)
    periodo_label = periodo or (f"{fecha_from} a {fecha_to}" if fecha_from else "Todos")

    xlsx_bytes = generate_libro_diario_xlsx(entries, org_info["name"], org_info["nit"], periodo_label)

    filename = f"Libro_Diario_{periodo or 'completo'}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/estado-resultados/export")
async def export_estado_resultados(
    periodo: str = Query(None, description="MMYYYY o YYYY"),
    fecha_corte: str = Query(None, description="Fecha de corte YYYY-MM-DD"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Exportar Estado de Resultados en XLSX profesional."""
    org_id = user.get("org_id")

    # Parse periodo to fecha_corte (use end of period)
    if periodo and not fecha_corte:
        _, fecha_corte = _parse_periodo(periodo)

    # Get balance using existing service function
    result = await get_balance_general(supabase, org_id, fecha_corte=fecha_corte)
    cuentas = result.get("cuentas", [])

    if not cuentas:
        raise HTTPException(status_code=404, detail="No hay datos contables para el período")

    org_info = _get_org_info(supabase, org_id)
    periodo_label = periodo or (f"Al {fecha_corte}" if fecha_corte else "Acumulado")

    xlsx_bytes = generate_estado_resultados_xlsx(cuentas, org_info["name"], org_info["nit"], periodo_label)

    filename = f"Estado_Resultados_{periodo or 'completo'}.xlsx"
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/partidas/{entry_id}/pdf")
async def export_partida_pdf(
    entry_id: str,
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
):
    """Genera PDF profesional de una partida contable individual."""
    org_id = user.get("org_id")

    entry = supabase.table("journal_entries") \
        .select("*").eq("id", entry_id).eq("org_id", org_id).execute()
    if not entry.data:
        raise HTTPException(status_code=404, detail="Partida no encontrada")

    lines = supabase.table("journal_entry_lines") \
        .select("*").eq("journal_entry_id", entry_id).eq("org_id", org_id) \
        .order("debe", desc=True).execute()

    org_info = _get_org_info(supabase, org_id)

    from app.services.contabilidad_pdf_service import generate_partida_pdf
    pdf_bytes = generate_partida_pdf(entry.data[0], lines.data or [], org_info)

    numero = entry.data[0].get("numero", "")
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=Partida_{numero}.pdf"},
    )


@router.post("/firmar-reporte")
async def firmar_reporte(
    tipo: str = Query(..., pattern="^(libro_diario|balance|estado_resultados)$"),
    periodo: str = Query(..., description="MMYYYY"),
    supabase=Depends(get_supabase),
    user=Depends(get_current_user),
    encryption=Depends(get_encryption),
):
    """Firma digital de reportes contables con el .p12 del emisor."""
    org_id = user["org_id"]

    # 1. Generate the report data
    fecha_from, fecha_to = _parse_periodo(periodo)
    org_info = _get_org_info(supabase, org_id)

    if tipo == "libro_diario":
        result = await list_journal_entries(supabase, org_id, fecha_from=fecha_from, fecha_to=fecha_to, per_page=9999)
        entries = result.get("data", [])
        if not entries:
            raise HTTPException(404, "No hay partidas para el período")
        report_bytes = generate_libro_diario_xlsx(entries, org_info["name"], org_info["nit"], periodo)
        report_title = "LIBRO DIARIO"
    elif tipo == "balance":
        result = await get_balance_general(supabase, org_id, fecha_corte=fecha_to)
        cuentas = result.get("cuentas", [])
        if not cuentas:
            raise HTTPException(404, "No hay datos contables para el período")
        report_bytes = generate_estado_resultados_xlsx(cuentas, org_info["name"], org_info["nit"], periodo)
        report_title = "BALANCE DE COMPROBACIÓN"
    else:  # estado_resultados
        result = await get_balance_general(supabase, org_id, fecha_corte=fecha_to)
        cuentas = result.get("cuentas", [])
        if not cuentas:
            raise HTTPException(404, "No hay datos contables para el período")
        report_bytes = generate_estado_resultados_xlsx(cuentas, org_info["name"], org_info["nit"], periodo)
        report_title = "ESTADO DE RESULTADOS"

    # 2. Hash the report content
    content_hash = hashlib.sha256(report_bytes).hexdigest()

    # 3. Load certificate and sign
    creds = supabase.table("mh_credentials").select(
        "certificate_encrypted, cert_password_encrypted, nombre, nit"
    ).eq("org_id", org_id).single().execute()

    if not creds.data or not creds.data.get("certificate_encrypted"):
        raise HTTPException(400, "No hay certificado .p12 configurado. Configure sus credenciales primero.")

    try:
        cert_bytes = encryption.decrypt(bytes.fromhex(creds.data["certificate_encrypted"]), org_id)
        cert_pwd = encryption.decrypt_string(bytes.fromhex(creds.data["cert_password_encrypted"]), org_id)
    except Exception:
        raise HTTPException(400, "Error al desencriptar el certificado. Verifique sus credenciales.")

    from app.modules.sign_engine import sign_engine
    cert_session = sign_engine.load_certificate(cert_bytes, cert_pwd)
    try:
        signature = sign_engine.sign_raw(cert_session, content_hash.encode("utf-8"))
    finally:
        cert_session.destroy()

    firmante_nombre = creds.data.get("nombre", "")
    firmante_nit = creds.data.get("nit", "")
    fecha_firma = datetime.now().isoformat()

    # 4. Generate signed PDF with signature page
    from app.services.contabilidad_pdf_service import generate_partida_pdf
    from fpdf import FPDF

    pdf = FPDF(orientation="P", unit="mm", format="Letter")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Signature verification page
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(79, 70, 229)
    pdf.cell(0, 10, "CERTIFICADO DE FIRMA DIGITAL", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(31, 41, 55)

    fields = [
        ("Tipo de reporte", report_title),
        ("Período", periodo),
        ("Empresa", org_info.get("name", "")),
        ("NIT Empresa", org_info.get("nit", "")),
        ("Firmante", firmante_nombre),
        ("NIT Firmante", firmante_nit),
        ("Fecha de firma", fecha_firma),
        ("Hash SHA-256 del contenido", ""),
    ]

    for label, value in fields:
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(55, 7, f"{label}:", new_x="RIGHT")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 7, value, new_x="LMARGIN", new_y="NEXT")

    # Hash in monospace on its own line
    pdf.set_font("Courier", "", 8)
    pdf.set_text_color(79, 70, 229)
    pdf.cell(0, 6, content_hash, new_x="LMARGIN", new_y="NEXT")

    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(31, 41, 55)
    pdf.cell(55, 7, "Firma digital (RS512):", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Courier", "", 6)
    pdf.set_text_color(100, 100, 100)
    # Wrap signature across lines
    sig = signature
    while sig:
        pdf.cell(0, 4, sig[:100], new_x="LMARGIN", new_y="NEXT")
        sig = sig[100:]

    pdf.ln(10)
    pdf.set_draw_color(79, 70, 229)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(5)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(128, 128, 128)
    pdf.cell(0, 5, "Este documento fue firmado digitalmente con el certificado .p12 del emisor.", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Para verificar la integridad, recalcule el SHA-256 del reporte XLSX adjunto.", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, f"Generado por FACTURA-SV — {datetime.now().strftime('%d/%m/%Y %H:%M')}", align="C")

    sig_pdf_bytes = io.BytesIO()
    pdf.output(sig_pdf_bytes)
    signed_pdf = sig_pdf_bytes.getvalue()

    # 5. Save record
    try:
        supabase.table("signed_reports").insert({
            "org_id": org_id,
            "tipo": tipo,
            "periodo": periodo,
            "content_hash": content_hash,
            "signature": signature[:500],
            "signed_by": user["user_id"],
            "signed_at": fecha_firma,
        }).execute()
    except Exception as e:
        logger.warning(f"Could not save signed_reports record: {e}")

    filename = f"{tipo}_{periodo}_firmado.pdf"
    return StreamingResponse(
        io.BytesIO(signed_pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
