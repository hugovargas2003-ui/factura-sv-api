"""
FACTURA-SV: Generador de PDF para DTEs
=======================================
Genera representación gráfica conforme a MH con QR de verificación.
Soporta todos los tipos de DTE.
"""
import io
import tempfile
from fpdf import FPDF
import qrcode

DTE_NOMBRES = {
    "01": "FACTURA", "03": "COMPROBANTE DE CRÉDITO FISCAL",
    "04": "NOTA DE REMISIÓN", "05": "NOTA DE CRÉDITO",
    "06": "NOTA DE DÉBITO", "07": "COMPROBANTE DE RETENCIÓN",
    "08": "COMPROBANTE DE LIQUIDACIÓN", "09": "DOC. CONTABLE DE LIQUIDACIÓN",
    "11": "FACTURA DE EXPORTACIÓN", "14": "FACTURA DE SUJETO EXCLUIDO",
    "15": "COMPROBANTE DE DONACIÓN",
}

MH_VERIFY_URL = "https://admin.factura.gob.sv/consultaPublica?ambiente={ambiente}&codGen={codGen}&fechaEmi={fechaEmi}"


class DTEPdfGenerator:
    """Genera PDF de representación gráfica de un DTE."""

    def __init__(self, dte_json: dict, sello: str | None = None,
                 estado: str = "procesado", logo_bytes: bytes | None = None,
                 primary_color: tuple | None = None):
        self.dte = dte_json
        self.sello = sello
        self.estado = estado
        self.logo_bytes = logo_bytes
        self.primary_color = primary_color or (26, 60, 94)
        self.ident = dte_json.get("identificacion", {})
        self.emisor = dte_json.get("emisor", {})
        self.tipo_dte = self.ident.get("tipoDte", "01")
        # receptor varies by DTE type
        if self.tipo_dte == "14":
            self.receptor = dte_json.get("sujetoExcluido", {})
        elif self.tipo_dte == "15":
            self.receptor = dte_json.get("donante", {})
        else:
            self.receptor = dte_json.get("receptor", {})
        self.cuerpo = dte_json.get("cuerpoDocumento", [])
        self.resumen = dte_json.get("resumen", {})
        self.extension = dte_json.get("extension", {})

    def generate(self) -> bytes:
        pdf = FPDF(orientation="P", unit="mm", format="Letter")
        pdf.set_auto_page_break(auto=True, margin=20)
        pdf.add_page()

        self._header(pdf)
        self._emisor_section(pdf)
        self._receptor_section(pdf)
        self._items_table(pdf)
        self._resumen_section(pdf)
        self._sello_section(pdf)
        self._qr_section(pdf)
        self._footer(pdf)

        return pdf.output()

    def _header(self, pdf: FPDF):
        """Tipo de documento, número de control, código de generación."""
        nombre_dte = DTE_NOMBRES.get(self.tipo_dte, f"DTE TIPO {self.tipo_dte}")

        r, g, b = self.primary_color
        logo_h = 0
        if self.logo_bytes:
            try:
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    tmp.write(self.logo_bytes)
                    tmp_path = tmp.name
                pdf.image(tmp_path, x=10, y=10, h=14)
                logo_h = 16
                import os
                os.unlink(tmp_path)
            except Exception:
                pass

        # Title bar
        pdf.set_fill_color(r, g, b)
        pdf.rect(10, 10 + logo_h, 196, 14, "F")
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(255, 255, 255)
        pdf.set_xy(10, 12 + logo_h)
        pdf.cell(196, 10, f"DOCUMENTO TRIBUTARIO ELECTRÓNICO", align="C")

        pdf.set_xy(10, 26 + logo_h)
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(r, g, b)
        pdf.cell(196, 8, nombre_dte, align="C")
        pdf.ln(10)

        # Control info
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(60, 60, 60)
        y = pdf.get_y()
        col = 10
        fields = [
            ("Número de Control:", self.ident.get("numeroControl", "—")),
            ("Código de Generación:", self.ident.get("codigoGeneracion", "—")),
            ("Fecha de Emisión:", self.ident.get("fecEmi", "—")),
            ("Hora:", self.ident.get("horEmi", "—")),
            ("Modelo de Facturación:", f"Modelo {self.ident.get('tipoModelo', 1)}"),
            ("Tipo de Transmisión:", f"Tipo {self.ident.get('tipoOperacion', 1)}"),
        ]
        for i, (label, value) in enumerate(fields):
            row = i // 2
            col_offset = (i % 2) * 98
            pdf.set_xy(10 + col_offset, y + row * 5)
            pdf.set_font("Helvetica", "B", 8)
            pdf.cell(30, 5, label)
            pdf.set_font("Helvetica", "", 8)
            pdf.cell(60, 5, str(value))

        pdf.set_y(y + 16)

    def _section_title(self, pdf: FPDF, title: str):
        pdf.set_fill_color(230, 240, 250)
        pdf.set_font("Helvetica", "B", 9)
        r, g, b = self.primary_color
        pdf.set_text_color(r, g, b)
        pdf.cell(196, 6, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(40, 40, 40)

    def _field_row(self, pdf: FPDF, pairs: list[tuple[str, str]]):
        pdf.set_font("Helvetica", "", 8)
        x_start = 10
        col_w = 196 / len(pairs) if pairs else 196
        y = pdf.get_y()
        for i, (label, value) in enumerate(pairs):
            pdf.set_xy(x_start + i * col_w, y)
            pdf.set_font("Helvetica", "B", 8)
            pdf.cell(25, 5, label)
            pdf.set_font("Helvetica", "", 8)
            pdf.cell(col_w - 25, 5, _safe(value))
        pdf.ln(5)

    def _emisor_section(self, pdf: FPDF):
        self._section_title(pdf, "EMISOR")
        e = self.emisor
        self._field_row(pdf, [
            ("Nombre:", e.get("nombre", "—")),
            ("NIT:", e.get("nit", "—")),
        ])
        self._field_row(pdf, [
            ("NRC:", e.get("nrc", "—")),
            ("Actividad:", e.get("descActividad", "—")),
        ])
        nombre_com = e.get("nombreComercial")
        if nombre_com:
            self._field_row(pdf, [
                ("Comercial:", nombre_com),
                ("Teléfono:", e.get("telefono", "—")),
            ])
        dir_data = e.get("direccion", {})
        if isinstance(dir_data, dict):
            self._field_row(pdf, [
                ("Dirección:", dir_data.get("complemento", "—")),
            ])
        elif isinstance(dir_data, str):
            self._field_row(pdf, [("Dirección:", dir_data)])
        pdf.ln(2)

    def _receptor_section(self, pdf: FPDF):
        r = self.receptor
        if not r:
            return
        if self.tipo_dte == "14":
            title = "SUJETO EXCLUIDO"
        elif self.tipo_dte == "15":
            title = "DONANTE"
        else:
            title = "RECEPTOR"
        self._section_title(pdf, title)

        self._field_row(pdf, [
            ("Nombre:", r.get("nombre", "—")),
            ("NIT:", r.get("nit", r.get("numDocumento", "—"))),
        ])
        nrc = r.get("nrc")
        correo = r.get("correo")
        if nrc or correo:
            self._field_row(pdf, [
                ("NRC:", nrc or "—"),
                ("Correo:", correo or "—"),
            ])
        dir_r = r.get("direccion", {})
        if isinstance(dir_r, dict):
            comp = dir_r.get("complemento", "—")
            self._field_row(pdf, [("Dirección:", comp)])
        pdf.ln(2)

    def _items_table(self, pdf: FPDF):
        self._section_title(pdf, "DETALLE DE OPERACIÓN")
        items = self.cuerpo
        if isinstance(items, dict):
            # tipo 09 has cuerpoDocumento as object
            items = [items]
        if not items:
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(196, 6, "Sin items", new_x="LMARGIN", new_y="NEXT")
            return

        # Table header
        col_widths = [10, 70, 16, 22, 26, 26, 26]
        headers = ["#", "Descripción", "Cant", "P.Unit", "Gravada", "Exenta", "Total"]
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_fill_color(26, 60, 94)
        pdf.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            pdf.cell(col_widths[i], 5, h, border=1, fill=True,
                     align="C" if i > 0 else "C")
        pdf.ln()

        pdf.set_text_color(40, 40, 40)
        pdf.set_font("Helvetica", "", 7)
        fill = False
        for item in items:
            if fill:
                pdf.set_fill_color(245, 248, 252)
            else:
                pdf.set_fill_color(255, 255, 255)

            desc = item.get("descripcion", "—")
            num = str(item.get("numItem", ""))
            cant = _fmtn(item.get("cantidad", 1))
            precio = _fmtm(item.get("precioUni", 0))
            gravada = _fmtm(item.get("ventaGravada", item.get("compraGravada", 0)))
            exenta = _fmtm(item.get("ventaExenta", item.get("ventaNoSuj", 0)))

            # Total per item
            total_item = (
                item.get("ventaGravada", 0) or 0
            ) + (
                item.get("ventaExenta", 0) or 0
            ) + (
                item.get("ventaNoSuj", 0) or 0
            ) + (
                item.get("compraGravada", 0) or 0
            )
            total_s = _fmtm(total_item)

            # Check if description needs multi-line
            max_desc_w = col_widths[1]
            if pdf.get_string_width(desc) > max_desc_w - 2:
                desc = desc[:45] + "..."

            vals = [num, desc, cant, precio, gravada, exenta, total_s]
            aligns = ["C", "L", "C", "R", "R", "R", "R"]
            for i, v in enumerate(vals):
                pdf.cell(col_widths[i], 5, v, border=1, fill=True, align=aligns[i])
            pdf.ln()
            fill = not fill

        pdf.ln(2)

    def _resumen_section(self, pdf: FPDF):
        self._section_title(pdf, "RESUMEN")
        r = self.resumen
        if not r:
            return

        # Right-aligned totals box
        pdf.set_font("Helvetica", "", 8)
        box_x = 120
        box_w = 86
        label_w = 50
        val_w = 36

        totals = []
        if r.get("totalGravada"):
            totals.append(("Total Gravada:", r["totalGravada"]))
        if r.get("totalExenta"):
            totals.append(("Total Exenta:", r["totalExenta"]))
        if r.get("totalNoSuj"):
            totals.append(("Total No Sujeta:", r["totalNoSuj"]))
        if r.get("subTotal"):
            totals.append(("Sub Total:", r["subTotal"]))
        if r.get("ivaRete1") or r.get("iva"):
            totals.append(("IVA:", r.get("ivaRete1") or r.get("iva", 0)))

        # Final total - varies by type
        total_final = (
            r.get("montoTotalOperacion") or r.get("totalPagar")
            or r.get("totalCompra") or r.get("valorTotal") or 0
        )
        totals.append(("TOTAL:", total_final))

        for i, (label, val) in enumerate(totals):
            is_total = (i == len(totals) - 1)
            pdf.set_xy(box_x, pdf.get_y())
            if is_total:
                pdf.set_font("Helvetica", "B", 10)
                pdf.set_fill_color(26, 60, 94)
                pdf.set_text_color(255, 255, 255)
            else:
                pdf.set_font("Helvetica", "", 8)
                pdf.set_fill_color(245, 248, 252)
                pdf.set_text_color(40, 40, 40)
            pdf.cell(label_w, 6, label, border=1, fill=True, align="R")
            pdf.cell(val_w, 6, f"${_fmtm(val)}", border=1, fill=True, align="R")
            pdf.ln()

        pdf.set_text_color(40, 40, 40)

        # Condición de operación and pago
        cond = r.get("condicionOperacion")
        if cond:
            cond_text = {1: "Contado", 2: "A crédito", 3: "Otro"}.get(cond, str(cond))
            pdf.set_font("Helvetica", "", 8)
            pdf.set_xy(10, pdf.get_y())
            pdf.cell(50, 5, f"Condición: {cond_text}")

        # Observaciones
        obs = r.get("observaciones")
        if obs:
            pdf.ln(2)
            pdf.set_font("Helvetica", "I", 7)
            pdf.multi_cell(196, 4, f"Observaciones: {obs}")

        pdf.ln(3)

    def _sello_section(self, pdf: FPDF):
        if not self.sello:
            return
        self._section_title(pdf, "VALIDACIÓN MINISTERIO DE HACIENDA")
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(0, 100, 0)
        pdf.cell(196, 5, f"Estado: {self.estado.upper()}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(196, 5, f"Sello de Recepción: {self.sello}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(40, 40, 40)
        pdf.ln(3)

    def _qr_section(self, pdf: FPDF):
        """QR code for MH verification."""
        codigo_gen = self.ident.get("codigoGeneracion", "")
        fecha_emi = self.ident.get("fecEmi", "")
        ambiente = self.ident.get("ambiente", "00")

        url = MH_VERIFY_URL.format(
            ambiente=ambiente, codGen=codigo_gen, fechaEmi=fecha_emi
        )

        try:
            qr = qrcode.QRCode(version=1, box_size=4, border=1)
            qr.add_data(url)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")

            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                img.save(tmp.name)
                qr_size = 30
                x_pos = 10
                y_pos = pdf.get_y()
                if y_pos + qr_size + 10 > 260:
                    pdf.add_page()
                    y_pos = pdf.get_y()
                pdf.image(tmp.name, x=x_pos, y=y_pos, w=qr_size, h=qr_size)
                pdf.set_xy(x_pos + qr_size + 5, y_pos + 5)
                pdf.set_font("Helvetica", "", 7)
                pdf.cell(100, 4, "Verifique este documento en:", new_x="LMARGIN")
                pdf.set_xy(x_pos + qr_size + 5, y_pos + 10)
                pdf.set_font("Helvetica", "U", 7)
                pdf.set_text_color(26, 60, 94)
                pdf.cell(150, 4, "https://admin.factura.gob.sv/consultaPublica")
                pdf.set_text_color(40, 40, 40)
                pdf.set_y(y_pos + qr_size + 5)
        except Exception:
            pdf.set_font("Helvetica", "I", 7)
            pdf.cell(196, 5, f"Verificar en: {url}", new_x="LMARGIN", new_y="NEXT")

    def _footer(self, pdf: FPDF):
        pdf.set_y(-15)
        pdf.set_font("Helvetica", "I", 6)
        pdf.set_text_color(150, 150, 150)
        pdf.cell(196, 4, "Representación gráfica de Documento Tributario Electrónico | "
                 "Generado por FACTURA-SV | algoritmos.io", align="C")


def _safe(v) -> str:
    if v is None:
        return "—"
    return str(v)

def _fmtm(v) -> str:
    try:
        return f"{float(v):,.2f}"
    except (ValueError, TypeError):
        return "0.00"

def _fmtn(v) -> str:
    try:
        n = float(v)
        return str(int(n)) if n == int(n) else f"{n:.2f}"
    except (ValueError, TypeError):
        return "1"
