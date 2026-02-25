"""
f07_generator.py — Generador de Anexos F-07 (Declaración IVA) en formato CSV.

Location: app/services/f07_generator.py
⚠️ NEW FILE — does not modify any existing infrastructure.

Genera CSVs compatibles con el portal DGII (portaldgii.mh.gob.sv):
- Anexo 1: Ventas a Contribuyentes (CCF tipo 03, NC tipo 05, ND tipo 06)
- Anexo 2: Ventas a Consumidor Final (Factura tipo 01, FSE tipo 14) — agrupado por día

Formato estricto:
- Delimitador: punto y coma (;)
- Sin encabezados
- Todas las celdas entrecomilladas (QUOTE_ALL)
- Encoding: UTF-8 sin BOM
- Salto de línea: \\r\\n (Windows/DGII)
"""

import csv
import io
import zipfile
from calendar import monthrange
from collections import defaultdict
from datetime import datetime
from typing import Any


# ---------------------------------------------------------------------------
# Helpers de formato
# ---------------------------------------------------------------------------

def _fmt_monto(valor: Any) -> str:
    """Formatea monto a 2 decimales como string. None/vacío → '0.00'."""
    if valor is None:
        return "0.00"
    try:
        return f"{float(valor):.2f}"
    except (ValueError, TypeError):
        return "0.00"


def _fmt_fecha(fecha_iso: str) -> str:
    """Convierte YYYY-MM-DD → DD/MM/YYYY."""
    try:
        parts = fecha_iso.split("-")
        return f"{parts[2]}/{parts[1]}/{parts[0]}"
    except (IndexError, AttributeError):
        return fecha_iso or ""


def _safe_str(val: Any) -> str:
    """Retorna string seguro; None → ''."""
    if val is None:
        return ""
    return str(val).strip()


def _strip_guiones(val: Any) -> str:
    """Elimina guiones de NIT/NRC."""
    return _safe_str(val).replace("-", "")


def _generar_csv(filas: list[list[str]]) -> bytes:
    """
    Genera CSV con formato exacto DGII:
    - Delimitador: ;
    - Sin encabezados
    - Todas las celdas entre comillas (QUOTE_ALL)
    - Encoding: UTF-8 sin BOM
    - Salto de línea: \\r\\n
    """
    output = io.StringIO()
    writer = csv.writer(
        output,
        delimiter=";",
        quoting=csv.QUOTE_ALL,
        lineterminator="\r\n",
    )
    for fila in filas:
        writer.writerow([str(campo) for campo in fila])
    return output.getvalue().encode("utf-8")


def _determinar_tipo_operacion(resumen: dict) -> str:
    """
    Determina tipo de operación:
    1=Gravada, 2=Exenta, 3=No sujeta, 4=Exportación
    """
    gravada = float(resumen.get("totalGravada", 0) or 0)
    exenta = float(resumen.get("totalExenta", 0) or 0)
    no_suj = float(resumen.get("totalNoSuj", 0) or 0)

    if gravada > 0:
        return "1"
    elif exenta > 0:
        return "2"
    elif no_suj > 0:
        return "3"
    return "1"  # Default gravada


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _date_range(periodo: str) -> tuple[str, str]:
    """Convierte 'YYYYMM' en (date_from, date_to) para query."""
    year = int(periodo[:4])
    month = int(periodo[4:6])
    last_day = monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"
    return date_from, date_to


async def _fetch_dtes_con_json(
    supabase: Any, org_id: str, tipos: list[str], periodo: str
) -> list[dict]:
    """
    Fetch DTEs con documento_json para un periodo y tipos dados.
    Solo estado PROCESADO.
    """
    date_from, date_to = _date_range(periodo)

    result = (
        supabase.table("dtes")
        .select(
            "id, tipo_dte, fecha_emision, numero_control, "
            "codigo_generacion, sello_recibido, "
            "receptor_nombre, receptor_nit, receptor_nrc, "
            "monto_total, total_gravada, total_exenta, total_no_sujeta, iva, "
            "documento_json"
        )
        .eq("org_id", org_id)
        .in_("tipo_dte", tipos)
        .eq("estado", "PROCESADO")
        .gte("fecha_emision", date_from)
        .lte("fecha_emision", date_to)
        .order("fecha_emision")
        .execute()
    )

    return result.data or []


async def _fetch_emisor_nit(supabase: Any, org_id: str) -> str:
    """Obtener NIT del emisor para nombre de archivo."""
    try:
        r = (
            supabase.table("mh_credentials")
            .select("nit")
            .eq("org_id", org_id)
            .single()
            .execute()
        )
        if r.data:
            return _strip_guiones(r.data.get("nit", ""))
    except Exception:
        pass
    return "000000000000"


# ---------------------------------------------------------------------------
# ANEXO 1 — Ventas a Contribuyentes (CCF, NC, ND)
# ---------------------------------------------------------------------------

def _build_anexo1_row(correlativo: int, dte: dict) -> list[str]:
    """
    Construye una fila de 20 columnas para Anexo 1.
    Extrae datos del documento_json cuando está disponible,
    con fallback a las columnas denormalizadas.
    """
    doc = dte.get("documento_json") or {}
    ident = doc.get("identificacion", {})
    receptor = doc.get("receptor", {})
    resumen = doc.get("resumen", {})
    tipo = dte.get("tipo_dte", "")

    # Campos de identificación — preferir JSON, fallback a columnas
    fecha_emi = ident.get("fecEmi") or dte.get("fecha_emision", "")
    num_control = ident.get("numeroControl") or dte.get("numero_control", "")
    sello = ident.get("selloRecibido") or dte.get("sello_recibido", "")
    cod_gen = ident.get("codigoGeneracion") or dte.get("codigo_generacion", "")

    # Receptor
    nrc_receptor = _strip_guiones(receptor.get("nrc") or dte.get("receptor_nrc", ""))
    nit_receptor = _strip_guiones(receptor.get("nit") or dte.get("receptor_nit", ""))
    nombre_receptor = _safe_str(receptor.get("nombre") or dte.get("receptor_nombre", ""))

    # Montos (de resumen del JSON o columnas denormalizadas)
    total_exenta = float(resumen.get("totalExenta", 0) or dte.get("total_exenta", 0) or 0)
    total_no_suj = float(resumen.get("totalNoSuj", 0) or dte.get("total_no_sujeta", 0) or 0)
    total_gravada = float(resumen.get("totalGravada", 0) or dte.get("total_gravada", 0) or 0)

    # IVA (débito fiscal) — del resumen.tributos o columna iva
    iva_valor = 0.0
    tributos = resumen.get("tributos")
    if tributos and isinstance(tributos, list):
        for t in tributos:
            if t.get("codigo") == "20":  # IVA 13%
                iva_valor = float(t.get("valor", 0) or 0)
                break
    if iva_valor == 0:
        iva_valor = float(dte.get("iva", 0) or 0)

    # IVA retenido
    iva_retenido = float(resumen.get("ivaRete1", 0) or 0)

    # Notas de Crédito (tipo 05) → montos en NEGATIVO
    if tipo == "05":
        total_exenta = -abs(total_exenta)
        total_no_suj = -abs(total_no_suj)
        total_gravada = -abs(total_gravada)
        iva_valor = -abs(iva_valor)
        iva_retenido = -abs(iva_retenido) if iva_retenido != 0 else 0

    tipo_operacion = _determinar_tipo_operacion(resumen)

    # 20 columnas exactas
    return [
        str(correlativo),                     # A: Correlativo
        _fmt_fecha(fecha_emi),                # B: Fecha emisión DD/MM/YYYY
        "D",                                  # C: Clase documento (D=DTE)
        tipo,                                 # D: Tipo documento (03, 05, 06)
        "",                                   # E: Nº resolución (vacío para DTE)
        "",                                   # F: Serie (vacío para DTE)
        num_control,                          # G: Número de control
        sello,                                # H: Sello de recepción
        cod_gen,                              # I: Código de generación
        nrc_receptor,                         # J: NRC receptor
        nit_receptor,                         # K: NIT receptor
        nombre_receptor,                      # L: Nombre receptor
        _fmt_monto(total_exenta),             # M: Ventas exentas
        _fmt_monto(total_no_suj),             # N: Ventas no sujetas
        _fmt_monto(total_gravada),            # O: Ventas gravadas netas
        _fmt_monto(iva_valor),                # P: Débito fiscal (IVA)
        "0.00",                               # Q: Ventas terceros no domiciliados
        _fmt_monto(iva_retenido),             # R: IVA retenido
        tipo_operacion,                       # S: Tipo operación
        "1",                                  # T: Tipo ingreso Renta (1=Gravado)
    ]


async def generate_anexo1(
    supabase: Any, org_id: str, periodo: str
) -> bytes:
    """
    Genera Anexo 1 — Ventas a Contribuyentes.
    Retorna bytes del CSV.
    """
    dtes = await _fetch_dtes_con_json(supabase, org_id, ["03", "05", "06"], periodo)
    filas = []
    for idx, dte in enumerate(dtes, 1):
        filas.append(_build_anexo1_row(idx, dte))
    return _generar_csv(filas)


# ---------------------------------------------------------------------------
# ANEXO 2 — Ventas a Consumidor Final (Factura, FSE) — AGRUPADO POR DÍA
# ---------------------------------------------------------------------------

async def generate_anexo2(
    supabase: Any, org_id: str, periodo: str
) -> bytes:
    """
    Genera Anexo 2 — Ventas a Consumidor Final, agrupado por día.
    Retorna bytes del CSV.
    """
    dtes = await _fetch_dtes_con_json(supabase, org_id, ["01", "14"], periodo)

    # Agrupar por (fecha, tipo_dte)
    grupos: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for dte in dtes:
        fecha = dte.get("fecha_emision", "")
        tipo = dte.get("tipo_dte", "01")
        grupos[(fecha, tipo)].append(dte)

    filas = []
    correlativo = 0

    for (fecha, tipo), grupo in sorted(grupos.items()):
        correlativo += 1

        # Ordenar por numero_control para determinar primero/último
        grupo_sorted = sorted(grupo, key=lambda d: d.get("numero_control", ""))
        primero = grupo_sorted[0]
        ultimo = grupo_sorted[-1]

        doc_primero = primero.get("documento_json") or {}
        doc_ultimo = ultimo.get("documento_json") or {}
        ident_primero = doc_primero.get("identificacion", {})
        ident_ultimo = doc_ultimo.get("identificacion", {})

        # Primer y último control/generación/sello
        ctrl_primero = ident_primero.get("numeroControl") or primero.get("numero_control", "")
        ctrl_ultimo = ident_ultimo.get("numeroControl") or ultimo.get("numero_control", "")
        gen_primero = ident_primero.get("codigoGeneracion") or primero.get("codigo_generacion", "")
        gen_ultimo = ident_ultimo.get("codigoGeneracion") or ultimo.get("codigo_generacion", "")
        sello_primero = ident_primero.get("selloRecibido") or primero.get("sello_recibido", "")
        sello_ultimo = ident_ultimo.get("selloRecibido") or ultimo.get("sello_recibido", "")

        # Sumar totales del día
        sum_exenta = 0.0
        sum_no_suj = 0.0
        sum_gravada = 0.0
        sum_iva_percibido = 0.0

        for dte in grupo:
            doc = dte.get("documento_json") or {}
            resumen = doc.get("resumen", {})

            exenta = float(resumen.get("totalExenta", 0) or dte.get("total_exenta", 0) or 0)
            no_suj = float(resumen.get("totalNoSuj", 0) or dte.get("total_no_sujeta", 0) or 0)

            if tipo == "01":
                # Tipo 01: ventaGravada INCLUYE IVA → usar montoTotalOperacion
                gravada = float(
                    resumen.get("montoTotalOperacion", 0)
                    or dte.get("monto_total", 0)
                    or 0
                )
            elif tipo == "14":
                # Tipo 14 (FSE): no tiene IVA, ventas van como gravadas
                gravada = float(resumen.get("totalGravada", 0) or dte.get("total_gravada", 0) or 0)
            else:
                gravada = float(resumen.get("totalGravada", 0) or dte.get("total_gravada", 0) or 0)

            iva_perc = float(resumen.get("ivaPerci1", 0) or 0)

            sum_exenta += exenta
            sum_no_suj += no_suj
            sum_gravada += gravada
            sum_iva_percibido += iva_perc

        # Total ventas del día = exenta + exenta_no_suj + no_suj + gravada + exportaciones
        sum_internas_exentas_no_suj = 0.0  # Col P: ventas internas exentas no sujetas
        sum_exportaciones = 0.0
        total_ventas = sum_exenta + sum_internas_exentas_no_suj + sum_no_suj + sum_gravada + sum_exportaciones

        tipo_operacion = "1" if sum_gravada > 0 else ("2" if sum_exenta > 0 else "3")

        # 23 columnas exactas
        fila = [
            str(correlativo),                      # A: Correlativo
            _fmt_fecha(fecha),                     # B: Fecha
            "D",                                   # C: Clase documento
            tipo,                                  # D: Tipo documento (01 o 14)
            "",                                    # E: Nº resolución
            "",                                    # F: Serie
            ctrl_primero,                          # G: Del número (primer control)
            ctrl_ultimo,                           # H: Al número (último control)
            gen_primero,                           # I: Código generación primero
            gen_ultimo,                            # J: Código generación último
            sello_primero,                         # K: Sello recepción primero
            sello_ultimo,                          # L: Sello recepción último
            "",                                    # M: NRC (vacío para consumidor final)
            "Varios",                              # N: Nombre cliente
            _fmt_monto(sum_exenta),                # O: Ventas exentas
            _fmt_monto(sum_internas_exentas_no_suj),  # P: Ventas internas exentas no sujetas
            _fmt_monto(sum_no_suj),                # Q: Ventas no sujetas
            _fmt_monto(sum_gravada),               # R: Ventas gravadas (IVA incluido para 01)
            _fmt_monto(sum_exportaciones),          # S: Exportaciones
            _fmt_monto(total_ventas),              # T: Total ventas del día
            _fmt_monto(sum_iva_percibido),          # U: IVA percibido
            tipo_operacion,                        # V: Tipo operación
            "1",                                   # W: Tipo ingreso Renta
        ]
        filas.append(fila)

    return _generar_csv(filas)


# ---------------------------------------------------------------------------
# ZIP con ambos anexos
# ---------------------------------------------------------------------------

async def generate_f07_zip(
    supabase: Any, org_id: str, periodo: str
) -> tuple[bytes, str]:
    """
    Genera ZIP con Anexo 1 y Anexo 2.
    Retorna (zip_bytes, filename).
    """
    nit = await _fetch_emisor_nit(supabase, org_id)

    anexo1_bytes = await generate_anexo1(supabase, org_id, periodo)
    anexo2_bytes = await generate_anexo2(supabase, org_id, periodo)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"Anexo1_Ventas_Contribuyentes_{periodo}.csv", anexo1_bytes)
        zf.writestr(f"Anexo2_Ventas_ConsumidorFinal_{periodo}.csv", anexo2_bytes)

    zip_buffer.seek(0)
    filename = f"F07_{periodo}_{nit}.zip"
    return zip_buffer.getvalue(), filename
