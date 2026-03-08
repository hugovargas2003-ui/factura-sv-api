"""
Contabilidad PDF Service — Genera PDFs de partidas contables.
Usa FPDF (misma librería que pdf_generator.py para DTEs).
"""
import io
from fpdf import FPDF


INDIGO = (79, 70, 229)
GRAY_LIGHT = (249, 250, 251)
GRAY_HEADER = (243, 244, 246)
GRAY_BORDER = (209, 213, 219)
GREEN = (5, 150, 105)
RED = (220, 38, 38)
WHITE = (255, 255, 255)
DARK = (31, 41, 55)


def generate_partida_pdf(entry: dict, lines: list, org_info: dict) -> bytes:
    """Generate professional PDF for a single journal entry."""
    pdf = FPDF(orientation="P", unit="mm", format="Letter")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Company header
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(*DARK)
    pdf.cell(0, 8, org_info.get("name", "FACTURA-SV"), align="C", new_x="LMARGIN", new_y="NEXT")

    nit = org_info.get("nit", "")
    if nit:
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(128, 128, 128)
        pdf.cell(0, 5, f"NIT: {nit}", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(6)

    # Entry title
    numero = entry.get("numero", "?")
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(*DARK)
    pdf.cell(0, 7, f"PARTIDA CONTABLE #{numero}", align="C", new_x="LMARGIN", new_y="NEXT")

    fecha = entry.get("fecha", "")
    tipo = entry.get("tipo", "manual")
    estado = entry.get("estado", "")
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(128, 128, 128)
    pdf.cell(0, 5, f"Fecha: {fecha}  |  Tipo: {tipo}  |  Estado: {estado}", align="C", new_x="LMARGIN", new_y="NEXT")

    desc = entry.get("descripcion", "")
    if desc:
        pdf.ln(3)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*DARK)
        pdf.cell(0, 5, f"Concepto: {desc}", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(8)

    # Table header
    col_widths = [22, 90, 35, 35]
    headers = ["Codigo", "Cuenta", "Debe", "Haber"]

    pdf.set_fill_color(*INDIGO)
    pdf.set_text_color(*WHITE)
    pdf.set_font("Helvetica", "B", 9)
    for i, h in enumerate(headers):
        align = "R" if i >= 2 else "L"
        pdf.cell(col_widths[i], 7, h, border=1, align=align, fill=True)
    pdf.ln()

    # Table rows
    total_debe = 0.0
    total_haber = 0.0

    for row_idx, line in enumerate(lines):
        debe = float(line.get("debe", 0))
        haber = float(line.get("haber", 0))
        total_debe += debe
        total_haber += haber

        cuenta_nombre = line.get("cuenta_nombre", "")
        if haber > 0 and debe == 0:
            cuenta_nombre = f"    {cuenta_nombre}"

        # Alternating row background
        if row_idx % 2 == 1:
            pdf.set_fill_color(*GRAY_LIGHT)
            fill = True
        else:
            pdf.set_fill_color(*WHITE)
            fill = True

        pdf.set_text_color(*DARK)
        pdf.set_font("Helvetica", "", 9)

        pdf.cell(col_widths[0], 6, line.get("cuenta_codigo", ""), border="LR", fill=fill)
        pdf.cell(col_widths[1], 6, cuenta_nombre[:55], border="LR", fill=fill)
        pdf.cell(col_widths[2], 6, f"${debe:,.2f}" if debe > 0 else "", border="LR", align="R", fill=fill)
        pdf.cell(col_widths[3], 6, f"${haber:,.2f}" if haber > 0 else "", border="LR", align="R", fill=fill)
        pdf.ln()

    # Totals row
    pdf.set_fill_color(*GRAY_HEADER)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(*DARK)
    pdf.set_draw_color(*DARK)
    pdf.cell(col_widths[0], 7, "", border="TLB", fill=True)
    pdf.cell(col_widths[1], 7, "TOTALES", border="TB", fill=True)
    pdf.cell(col_widths[2], 7, f"${total_debe:,.2f}", border="TB", align="R", fill=True)
    pdf.cell(col_widths[3], 7, f"${total_haber:,.2f}", border="TRB", align="R", fill=True)
    pdf.ln()

    # Balance check
    pdf.ln(8)
    diff = abs(total_debe - total_haber)
    if diff < 0.01:
        pdf.set_text_color(*GREEN)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, "Partida balanceada", new_x="LMARGIN", new_y="NEXT")
    else:
        pdf.set_text_color(*RED)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, f"Diferencia: ${diff:,.2f}", new_x="LMARGIN", new_y="NEXT")

    # Footer
    pdf.ln(20)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(180, 180, 180)
    from datetime import datetime
    pdf.cell(0, 4, f"Generado por FACTURA-SV — {datetime.now().strftime('%d/%m/%Y %H:%M')}", align="C")

    buf = io.BytesIO()
    pdf.output(buf)
    return buf.getvalue()
