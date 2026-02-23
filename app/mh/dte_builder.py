"""
FACTURA-SV: Constructor de DTEs (13 tipos)
==========================================
Reescrito para coincidir EXACTAMENTE con los JSONs que fueron
PROCESADOS durante la certificacion con MH (600+ DTEs aceptados).

REGLAS CRITICAS NO DOCUMENTADAS:
- 01: ivaItem = ventaGravada - ventaGravada/1.13 (NO * 0.13)
- 01: montoTotalOperacion = totalGravada (precio IVA-inclusive)
- 04: receptor usa tipoDocumento/numDocumento + bienTitulo obligatorio
- 05/06: resumen NO puede tener pagos, totalPagar, saldoFavor, etc.
- 05/06: emisor NO lleva codEstableMH/codEstable
- 07: emisor sin tipoEstablecimiento, con distrito, codEstable sin MH
- 07: totalIvaRetenido (no totalIVAretenido), totalIva=0.0 requerido
- 08: version 1, documentoRelacionado es array
- 09: cuerpoDocumento es objeto, porcentComision=integer
- 11: motivoContigencia (con g), emisor con tipoItemExpor
- 14: usa sujetoExcluido (no receptor), emisor sin nombreComercial
- 15: usa donante/donatario (no emisor/receptor)
"""
import uuid
from datetime import date, datetime
from typing import Any


def _uuid() -> str:
    return str(uuid.uuid4()).upper()

def _now_date() -> str:
    return date.today().isoformat()

def _now_time() -> str:
    return datetime.now().strftime("%H:%M:%S")

DTE_VERSIONS: dict[str, int] = {
    "01": 1, "03": 3, "04": 3, "05": 3, "06": 3,
    "07": 2, "08": 1, "09": 1, "11": 1, "14": 1, "15": 1,
}


class DTEBuilder:
    def __init__(self, emisor: dict, ambiente: str = "00"):
        self.emisor = emisor
        self.ambiente = ambiente

    def build(self, tipo_dte: str, numero_control: str, receptor: dict,
              items: list[dict], *, condicion_operacion: int = 1,
              observaciones: str | None = None, dte_referencia: dict | None = None,
              extension: dict | None = None, dcl_params: dict | None = None,
              cd_params: dict | None = None) -> tuple[dict, str]:
        codigo_gen = _uuid()
        version = DTE_VERSIONS.get(tipo_dte, 3)
        builders = {
            "01": self._build_factura, "03": self._build_ccf,
            "04": self._build_nr, "05": self._build_nc, "06": self._build_nd,
            "07": self._build_cr, "08": self._build_cl, "09": self._build_dcl,
            "11": self._build_fexe, "14": self._build_fse, "15": self._build_cd,
        }
        fn = builders.get(tipo_dte)
        if not fn:
            raise ValueError(f"Tipo DTE no soportado: {tipo_dte}")
        dte = fn(version=version, numero_control=numero_control,
                 codigo_generacion=codigo_gen, receptor=receptor, items=items,
                 condicion_operacion=condicion_operacion, observaciones=observaciones,
                 dte_referencia=dte_referencia, extension=extension,
                 dcl_params=dcl_params, cd_params=cd_params)
        return dte, codigo_gen

    # === FACTURA (01) v1 ===
    def _build_factura(self, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        cuerpo = []
        for i, item in enumerate(items, 1):
            precio = round(item["precio_unitario"], 2)
            cant = item.get("cantidad", 1)
            vg = round(precio * cant, 2)
            iva_item = round(vg - vg / 1.13, 2)
            cuerpo.append({
                "numItem": i, "tipoItem": item.get("tipo_item", 2),
                "numeroDocumento": None, "codigo": item.get("codigo"),
                "codTributo": None, "cantidad": float(cant),
                "uniMedida": item.get("unidad_medida", 59),
                "descripcion": item["descripcion"], "precioUni": precio,
                "montoDescu": round(float(item.get("descuento", 0)), 2),
                "ventaNoSuj": 0.0, "ventaExenta": 0.0, "ventaGravada": vg,
                "tributos": None, "psv": 0.0, "noGravado": 0.0,
                "ivaItem": iva_item,
            })
        tg = round(sum(c["ventaGravada"] for c in cuerpo), 2)
        ti = round(sum(c["ivaItem"] for c in cuerpo), 2)
        resumen = {
            "totalNoSuj": 0.0, "totalExenta": 0.0, "totalGravada": tg,
            "subTotalVentas": tg, "descuNoSuj": 0.0, "descuExenta": 0.0,
            "descuGravada": 0.0, "porcentajeDescuento": 0.0, "totalDescu": 0.0,
            "tributos": None, "subTotal": tg, "ivaRete1": 0.0, "reteRenta": 0.0,
            "montoTotalOperacion": tg, "totalNoGravado": 0.0, "totalPagar": tg,
            "totalLetras": self._monto_letras(tg), "totalIva": ti,
            "saldoFavor": 0.0, "condicionOperacion": kw.get("condicion_operacion", 1),
            "pagos": [{"codigo": "01", "montoPago": tg, "referencia": "", "plazo": None, "periodo": None}],
            "numPagoElectronico": None,
        }
        return self._wrap_std("01", kw, self._rec_factura(receptor), cuerpo, resumen)

    # === CCF (03) v3 ===
    def _build_ccf(self, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        cuerpo = []
        for i, item in enumerate(items, 1):
            precio = round(item["precio_unitario"], 2)
            cant = item.get("cantidad", 1)
            vg = round(precio * cant, 2)
            cuerpo.append({
                "numItem": i, "tipoItem": item.get("tipo_item", 2),
                "numeroDocumento": None, "codigo": item.get("codigo"),
                "codTributo": None, "cantidad": float(cant),
                "uniMedida": item.get("unidad_medida", 59),
                "descripcion": item["descripcion"], "precioUni": precio,
                "montoDescu": round(float(item.get("descuento", 0)), 2),
                "ventaNoSuj": 0.0, "ventaExenta": 0.0, "ventaGravada": vg,
                "tributos": ["20"] if vg > 0 else None, "psv": 0.0, "noGravado": 0.0,
            })
        tg = round(sum(c["ventaGravada"] for c in cuerpo), 2)
        iva = round(tg * 0.13, 2)
        mt = round(tg + iva, 2)
        resumen = {
            "totalNoSuj": 0.0, "totalExenta": 0.0, "totalGravada": tg,
            "subTotalVentas": tg, "descuNoSuj": 0.0, "descuExenta": 0.0,
            "descuGravada": 0.0, "porcentajeDescuento": 0.0, "totalDescu": 0.0,
            "tributos": [{"codigo": "20", "descripcion": "Impuesto al Valor Agregado 13%", "valor": iva}],
            "subTotal": tg, "ivaPerci1": 0.0, "ivaRete1": 0.0, "reteRenta": 0.0,
            "montoTotalOperacion": mt, "totalNoGravado": 0.0, "totalPagar": mt,
            "totalLetras": self._monto_letras(mt), "saldoFavor": 0.0,
            "condicionOperacion": kw.get("condicion_operacion", 1),
            "pagos": [{"codigo": "01", "montoPago": mt, "referencia": "", "plazo": None, "periodo": None}],
            "numPagoElectronico": None,
        }
        return self._wrap_std("03", kw, self._rec_ccf(receptor), cuerpo, resumen)

    # === NR (04) v3 ===
    def _build_nr(self, **kw) -> dict:
        """Nota de Remisión (04) v3 - Schema: fe-nr-v3.json
        Items: 14 fields only. Resumen: 13 fields only. No pagos/condicion."""
        items, receptor = kw["items"], kw["receptor"]
        cuerpo = []
        for i, item in enumerate(items, 1):
            precio = round(item.get("precio_unitario", 0), 2)
            cant = item.get("cantidad", 1)
            vg = round(precio * cant, 2)
            cuerpo.append({
                "numItem": i, "tipoItem": item.get("tipo_item", 2),
                "numeroDocumento": None, "codigo": item.get("codigo"),
                "codTributo": None, "descripcion": item["descripcion"],
                "cantidad": float(cant), "uniMedida": item.get("unidad_medida", 59),
                "precioUni": precio, "montoDescu": 0.0,
                "ventaNoSuj": 0.0, "ventaExenta": 0.0,
                "ventaGravada": vg, "tributos": ["20"] if vg > 0 else None,
            })
        tg = round(sum(c["ventaGravada"] for c in cuerpo), 2)
        iva = round(tg * 0.13, 2)
        mt = round(tg + iva, 2)
        resumen = {
            "totalNoSuj": 0.0, "totalExenta": 0.0, "totalGravada": tg,
            "subTotalVentas": tg, "descuNoSuj": 0.0, "descuExenta": 0.0,
            "descuGravada": 0.0, "porcentajeDescuento": 0.0, "totalDescu": 0.0,
            "tributos": [{"codigo": "20", "descripcion": "Impuesto al Valor Agregado 13%", "valor": iva}],
            "subTotal": tg, "montoTotalOperacion": mt,
            "totalLetras": self._monto_letras(mt),
        }
        rec = {
            "tipoDocumento": receptor.get("tipo_documento", "36"),
            "numDocumento": receptor.get("num_documento") or receptor.get("nit"),
            "nrc": receptor.get("nrc"), "nombre": receptor["nombre"],
            "codActividad": receptor.get("cod_actividad"),
            "descActividad": receptor.get("desc_actividad"),
            "nombreComercial": receptor.get("nombre_comercial"),
            "direccion": {"departamento": receptor.get("direccion_departamento", "06"),
                          "municipio": receptor.get("direccion_municipio", "14"),
                          "complemento": receptor.get("direccion_complemento", "San Salvador")},
            "telefono": receptor.get("telefono"), "correo": receptor.get("correo"),
            "bienTitulo": receptor.get("bien_titulo", "04"),
        }
        dte = self._ident(kw, "04")
        dte["documentoRelacionado"] = None
        dte["emisor"] = self._emisor_std()
        dte["receptor"] = rec
        dte["ventaTercero"] = None
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["extension"] = kw.get("extension")
        dte["apendice"] = None
        return dte

    # === NC (05) / ND (06) v3 ===
    def _build_nc(self, **kw): return self._build_nota("05", **kw)
    def _build_nd(self, **kw): return self._build_nota("06", **kw)

    def _build_nota(self, tipo: str, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        ref = kw.get("dte_referencia") or {}
        cuerpo = []
        for i, item in enumerate(items, 1):
            precio = round(item["precio_unitario"], 2)
            cant = item.get("cantidad", 1)
            vg = round(precio * cant, 2)
            cuerpo.append({
                "numItem": i, "tipoItem": item.get("tipo_item", 2),
                "numeroDocumento": ref.get("codigo_generacion"),
                "codigo": item.get("codigo"), "codTributo": None,
                "cantidad": float(cant), "uniMedida": item.get("unidad_medida", 59),
                "descripcion": item["descripcion"], "precioUni": precio,
                "montoDescu": 0.0, "ventaNoSuj": 0.0, "ventaExenta": 0.0,
                "ventaGravada": vg, "tributos": ["20"] if vg > 0 else None,
            })
        tg = round(sum(c["ventaGravada"] for c in cuerpo), 2)
        iva = round(tg * 0.13, 2)
        mt = round(tg + iva, 2)
        resumen = {
            "totalNoSuj": 0.0, "totalExenta": 0.0, "totalGravada": tg,
            "subTotalVentas": tg, "descuNoSuj": 0.0, "descuExenta": 0.0,
            "descuGravada": 0.0, "totalDescu": 0.0,
            "tributos": [{"codigo": "20", "descripcion": "Impuesto al Valor Agregado 13%", "valor": iva}],
            "subTotal": tg, "ivaPerci1": 0.0, "ivaRete1": 0.0, "reteRenta": 0.0,
            "montoTotalOperacion": mt,
            "totalLetras": self._monto_letras(mt),
            "condicionOperacion": kw.get("condicion_operacion", 1),
        }
        if tipo == "06":
            resumen["numPagoElectronico"] = None
        doc_rel = None
        if ref:
            doc_rel = [{"tipoDocumento": ref.get("tipo_dte", "03"),
                        "tipoGeneracion": ref.get("tipo_generacion", 2),
                        "numeroDocumento": ref.get("codigo_generacion"),
                        "fechaEmision": ref.get("fecha_emision", _now_date())}]
        e = self.emisor
        emisor_nota = {
            "nit": e["nit"], "nrc": e["nrc"], "nombre": e["nombre"],
            "codActividad": e["cod_actividad"], "descActividad": e["desc_actividad"],
            "nombreComercial": e.get("nombre_comercial"),
            "tipoEstablecimiento": e.get("tipo_establecimiento", "01"),
            "direccion": {"departamento": e["direccion_departamento"],
                          "municipio": e["direccion_municipio"],
                          "complemento": e["direccion_complemento"]},
            "telefono": e["telefono"], "correo": e["correo"],
        }
        dte = self._ident(kw, tipo)
        dte["documentoRelacionado"] = doc_rel
        dte["emisor"] = emisor_nota
        dte["receptor"] = self._rec_ccf(receptor)
        dte["ventaTercero"] = None
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["extension"] = kw.get("extension")
        dte["apendice"] = None
        return dte

    # === CR (07) v2 ===
    def _build_cr(self, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        cuerpo = []
        for i, item in enumerate(items, 1):
            monto = round(item["monto_sujeto"], 2)
            iva_ret = round(item.get("iva_retenido", monto * 0.01), 2)
            cuerpo.append({
                "numItem": i, "tipoDte": item.get("tipo_dte_ref", "03"),
                "tipoGeneracion": item.get("tipo_generacion", 1),
                "numDocumento": item.get("num_documento", "00010001000000001"),
                "fechaEmision": item.get("fecha_emision", _now_date()),
                "montoSujetoGrav": monto,
                "codigoRetencionMH": item.get("codigo_retencion", "22"),
                "ivaRetenido": iva_ret,
                "descripcion": item.get("descripcion", "Retencion IVA"),
            })
        tr = round(sum(c["ivaRetenido"] for c in cuerpo), 2)
        ts = round(sum(c["montoSujetoGrav"] for c in cuerpo), 2)
        resumen = {
            "totalSujetoRetencion": ts, "totalIvaRetenido": tr,
            "totalLetras": self._monto_letras(tr),
            "totalIva": 0.0, "observaciones": kw.get("observaciones"),
        }
        e = self.emisor
        emisor_cr = {
            "nit": e["nit"], "nrc": e["nrc"], "nombre": e["nombre"],
            "codActividad": e["cod_actividad"], "descActividad": e["desc_actividad"],
            "nombreComercial": e.get("nombre_comercial"),
            "direccion": {"departamento": e["direccion_departamento"],
                          "municipio": e["direccion_municipio"],
                          "distrito": e.get("direccion_distrito", "01"),
                          "complemento": e["direccion_complemento"]},
            "telefono": e["telefono"], "correo": e["correo"],
            "codEstable": e.get("codigo_establecimiento", "M001"),
            "codPuntoVenta": e.get("codigo_punto_venta", "P001"),
        }
        rec = {
            "tipoDocumento": receptor.get("tipo_documento", "36"),
            "numDocumento": receptor.get("num_documento") or receptor.get("nit"),
            "nrc": receptor.get("nrc"), "nombre": receptor["nombre"],
            "nombreComercial": receptor.get("nombre_comercial"),
            "codActividad": receptor.get("cod_actividad"),
            "descActividad": receptor.get("desc_actividad"),
            "direccion": {"departamento": receptor.get("direccion_departamento", "06"),
                          "municipio": receptor.get("direccion_municipio", "20"),
                          "distrito": receptor.get("direccion_distrito", "01"),
                          "complemento": receptor.get("direccion_complemento", "San Salvador")},
            "telefono": receptor.get("telefono"), "correo": receptor.get("correo"),
        }
        dte = self._ident(kw, "07")
        dte["emisor"] = emisor_cr
        dte["receptor"] = rec
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["apendice"] = None
        return dte

    # === CL (08) v1 ===
    def _build_cl(self, **kw) -> dict:
        """Comprobante de Liquidación (08) v1 - Schema: fe-cl-v1.json
        Items are document references, NOT products.
        TOP: NO documentoRelacionado/otrosDocumentos/ventaTercero."""
        items, receptor = kw["items"], kw["receptor"]
        ref = kw.get("dte_referencia") or {}
        cuerpo = []
        for i, item in enumerate(items, 1):
            vg = round(item.get("precio_unitario", 0) * item.get("cantidad", 1), 2)
            iva_item = round(vg * 0.13, 2)
            cuerpo.append({
                "numItem": i,
                "tipoDte": ref.get("tipo_dte", "03"),
                "tipoGeneracion": ref.get("tipo_generacion", 1),
                "numeroDocumento": ref.get("codigo_generacion", "00010001000000001"),
                "fechaGeneracion": ref.get("fecha_emision", _now_date()),
                "ventaNoSuj": 0.0, "ventaExenta": 0.0,
                "ventaGravada": vg, "exportaciones": 0.0,
                "tributos": ["20"] if vg > 0 else None,
                "ivaItem": iva_item,
                "obsItem": item.get("descripcion", "Liquidacion"),
            })
        tg = round(sum(c["ventaGravada"] for c in cuerpo), 2)
        iva = round(sum(c["ivaItem"] for c in cuerpo), 2)
        mt = round(tg + iva, 2)
        resumen = {
            "totalNoSuj": 0.0, "totalExenta": 0.0, "totalGravada": tg,
            "totalExportacion": 0.0, "subTotalVentas": tg,
            "tributos": [{"codigo": "20", "descripcion": "Impuesto al Valor Agregado 13%", "valor": iva}],
            "montoTotalOperacion": mt, "ivaPerci": 0.0,
            "total": mt,
            "totalLetras": self._monto_letras(mt),
            "condicionOperacion": kw.get("condicion_operacion", 1),
        }
        dte = self._ident(kw, "08")
        dte["identificacion"].pop("tipoContingencia", None)
        dte["identificacion"].pop("motivoContin", None)
        dte["emisor"] = self._emisor_std()
        dte["receptor"] = self._rec_ccf(receptor)
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["extension"] = kw.get("extension")
        dte["apendice"] = None
        return dte

    # === DCL (09) v1 ===
    def _build_dcl(self, **kw) -> dict:
        receptor = kw["receptor"]
        params = kw.get("dcl_params") or {}
        val_op = round(params.get("valor_operaciones", 1130.0), 2)
        base = round(val_op / 1.13, 2)
        iva = round(val_op - base, 2)
        msp = base
        ip = round(msp * 0.02, 2)
        pct = params.get("porcentaje_comision", 5)
        com = round(val_op * pct / 100, 2)
        ic = round(com * 0.13, 2)
        liq = round(val_op - com - ic - ip, 2)
        cuerpo = {
            "periodoLiquidacionFechaInicio": params.get("fecha_inicio", _now_date()),
            "periodoLiquidacionFechaFin": params.get("fecha_fin", _now_date()),
            "codLiquidacion": params.get("codigo", "LIQ-0001"),
            "cantidadDoc": params.get("cantidad_docs", 10),
            "valorOperaciones": val_op, "montoSinPercepcion": 0.0,
            "descripSinPercepcion": None, "subTotal": val_op, "iva": iva,
            "montoSujetoPercepcion": msp, "ivaPercibido": ip,
            "comision": com, "porcentComision": round(float(pct), 2), "ivaComision": ic,
            "liquidoApagar": liq, "totalLetras": self._monto_letras(liq),
            "observaciones": kw.get("observaciones"),
        }
        e = self.emisor
        emisor_dcl = {
            "nit": e["nit"], "nrc": e["nrc"], "nombre": e["nombre"],
            "codActividad": e["cod_actividad"], "descActividad": e["desc_actividad"],
            "nombreComercial": e.get("nombre_comercial"),
            "tipoEstablecimiento": e.get("tipo_establecimiento", "01"),
            "direccion": {"departamento": e["direccion_departamento"],
                          "municipio": e["direccion_municipio"],
                          "complemento": e["direccion_complemento"]},
            "telefono": e["telefono"], "correo": e["correo"],
            "codigoMH": e.get("codigo_establecimiento", "M001"),
            "codigo": e.get("codigo_establecimiento", "M001"),
            "puntoVentaMH": e.get("codigo_punto_venta", "P001"),
            "puntoVentaContri": e.get("codigo_punto_venta", "P001"),
        }
        rec = self._rec_ccf(receptor)
        rec["tipoEstablecimiento"] = receptor.get("tipo_establecimiento", "01")
        rec["codigoMH"] = receptor.get("codigo_mh")
        rec["puntoVentaMH"] = receptor.get("punto_venta_mh")
        dte = self._ident(kw, "09")
        dte["identificacion"].pop("tipoContingencia", None)
        dte["identificacion"].pop("motivoContin", None)
        dte["emisor"] = emisor_dcl
        dte["receptor"] = rec
        dte["cuerpoDocumento"] = cuerpo
        dte["extension"] = kw.get("extension") or {
            "nombEntrega": e["nombre"], "docuEntrega": e["nit"], "codEmpleado": None}
        dte["apendice"] = None
        return dte

    # === FEXE (11) v1 ===
    def _build_fexe(self, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        cuerpo = []
        for i, item in enumerate(items, 1):
            precio = round(item["precio_unitario"], 2)
            cant = item.get("cantidad", 1)
            venta = round(precio * cant, 2)
            cuerpo.append({
                "numItem": i, "codigo": item.get("codigo"),
                "cantidad": float(cant), "uniMedida": item.get("unidad_medida", 59),
                "descripcion": item["descripcion"], "precioUni": precio,
                "montoDescu": 0.0, "ventaGravada": venta,
                "tributos": item.get("tributos_export", ["C3"]),
                "noGravado": 0.0,
            })
        tg = round(sum(c["ventaGravada"] for c in cuerpo), 2)
        resumen = {
            "totalGravada": tg, "descuento": 0.0, "porcentajeDescuento": 0.0,
            "totalDescu": 0.0, "montoTotalOperacion": tg, "totalNoGravado": 0.0,
            "totalPagar": tg, "totalLetras": self._monto_letras(tg),
            "condicionOperacion": kw.get("condicion_operacion", 1),
            "pagos": [{"codigo": "01", "montoPago": tg, "referencia": "", "plazo": None, "periodo": None}],
            "numPagoElectronico": None,
            "codIncoterms": None, "descIncoterms": None,
            "flete": 0.0, "seguro": 0.0, "observaciones": kw.get("observaciones"),
        }
        emisor_fexe = self._emisor_std()
        emisor_fexe["tipoItemExpor"] = 1
        emisor_fexe["recintoFiscal"] = None
        emisor_fexe["regimen"] = None
        rec = {
            "tipoDocumento": receptor.get("tipo_documento", "37"),
            "numDocumento": receptor.get("num_documento", "000000000"),
            "nombre": receptor["nombre"],
            "nombreComercial": receptor.get("nombre_comercial"),
            "codPais": receptor.get("cod_pais", "9300"),
            "nombrePais": receptor.get("nombre_pais", "ESTADOS UNIDOS"),
            "complemento": receptor.get("complemento", receptor.get("direccion_complemento", "Exterior")),
            "tipoPersona": receptor.get("tipo_persona", 1),
            "descActividad": receptor.get("desc_actividad", "Actividades varias"),
            "telefono": receptor.get("telefono"), "correo": receptor.get("correo"),
        }
        dte = {"identificacion": {
            "version": kw["version"], "ambiente": self.ambiente, "tipoDte": "11",
            "numeroControl": kw["numero_control"], "codigoGeneracion": kw["codigo_generacion"],
            "tipoModelo": 1, "tipoOperacion": 1, "tipoContingencia": None,
            "motivoContigencia": None, "fecEmi": _now_date(), "horEmi": _now_time(), "tipoMoneda": "USD"}}
        dte["emisor"] = emisor_fexe
        dte["receptor"] = rec
        dte["otrosDocumentos"] = None
        dte["ventaTercero"] = None
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["apendice"] = None
        return dte

    # === FSE (14) v1 ===
    def _build_fse(self, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        cuerpo = []
        for i, item in enumerate(items, 1):
            precio = round(item["precio_unitario"], 2)
            cant = item.get("cantidad", 1)
            cuerpo.append({
                "numItem": i, "tipoItem": item.get("tipo_item", 2),
                "codigo": item.get("codigo"), "cantidad": float(cant),
                "uniMedida": item.get("unidad_medida", 59),
                "descripcion": item["descripcion"], "precioUni": precio,
                "montoDescu": 0.0, "compra": round(precio * cant, 2),
            })
        tc = round(sum(c["compra"] for c in cuerpo), 2)
        rr = round(tc * 0.10, 2) if tc >= 100 else 0.0
        tp = round(tc - rr, 2)
        resumen = {
            "totalCompra": tc, "descu": 0.0, "totalDescu": 0.0, "subTotal": tc,
            "ivaRete1": 0.0, "reteRenta": rr, "totalPagar": tp,
            "totalLetras": self._monto_letras(tp),
            "condicionOperacion": kw.get("condicion_operacion", 1),
            "pagos": [{"codigo": "01", "montoPago": tp, "referencia": "", "plazo": None, "periodo": None}],
            "observaciones": kw.get("observaciones"),
        }
        e = self.emisor
        emisor_fse = {
            "nit": e["nit"], "nrc": e["nrc"], "nombre": e["nombre"],
            "codActividad": e["cod_actividad"], "descActividad": e["desc_actividad"],
            "direccion": {"departamento": e["direccion_departamento"],
                          "municipio": e["direccion_municipio"],
                          "complemento": e["direccion_complemento"]},
            "telefono": e["telefono"], "correo": e["correo"],
            "codEstableMH": e.get("codigo_establecimiento", "M001"),
            "codEstable": e.get("codigo_establecimiento", "M001"),
            "codPuntoVentaMH": e.get("codigo_punto_venta", "P001"),
            "codPuntoVenta": e.get("codigo_punto_venta", "P001"),
        }
        sujeto = {
            "tipoDocumento": receptor.get("tipo_documento", "13"),
            "numDocumento": receptor.get("num_documento", "000000000"),
            "nombre": receptor["nombre"],
            "codActividad": receptor.get("cod_actividad"),
            "descActividad": receptor.get("desc_actividad"),
            "direccion": {"departamento": receptor.get("direccion_departamento", "06"),
                          "municipio": receptor.get("direccion_municipio", "14"),
                          "complemento": receptor.get("direccion_complemento", "San Salvador")},
            "telefono": receptor.get("telefono"), "correo": receptor.get("correo"),
        }
        dte = self._ident(kw, "14")
        dte["emisor"] = emisor_fse
        dte["sujetoExcluido"] = sujeto
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["apendice"] = None
        return dte

    # === CD (15) v1 ===
    def _build_cd(self, **kw) -> dict:
        items, receptor = kw["items"], kw["receptor"]
        cd_p = kw.get("cd_params") or {}
        cuerpo = []
        for i, item in enumerate(items, 1):
            cuerpo.append({
                "numItem": i, "tipoItem": item.get("tipo_item", 1),
                "codigo": item.get("codigo"), "cantidad": float(item.get("cantidad", 1)),
                "uniMedida": 99, "descripcion": item["descripcion"],
                "valorDonacion": round(float(item.get("valor", item.get("precio_unitario", 100))), 2),
            })
        td = round(sum(c["valorDonacion"] for c in cuerpo), 2)
        resumen = {"totalDonacion": td, "totalLetras": self._monto_letras(td),
                    "condicionOperacion": kw.get("condicion_operacion", 1)}
        donante = {
            "tipoDocumento": receptor.get("tipo_documento", "36"),
            "numDocumento": receptor.get("num_documento") or receptor.get("nit"),
            "nrc": receptor.get("nrc"), "nombre": receptor["nombre"],
            "nombreComercial": receptor.get("nombre_comercial"),
            "codActividad": receptor.get("cod_actividad"),
            "descActividad": receptor.get("desc_actividad"),
            "direccion": {"departamento": receptor.get("direccion_departamento", "06"),
                          "municipio": receptor.get("direccion_municipio", "14"),
                          "complemento": receptor.get("direccion_complemento", "San Salvador")},
            "telefono": receptor.get("telefono"), "correo": receptor.get("correo"),
            "codPais": receptor.get("cod_pais", "9300"),
        }
        e = self.emisor
        donatario = {
            "nit": e["nit"], "nrc": e["nrc"], "nombre": e["nombre"],
            "nombreComercial": e.get("nombre_comercial"),
            "codActividad": e["cod_actividad"], "descActividad": e["desc_actividad"],
            "direccion": {"departamento": e["direccion_departamento"],
                          "municipio": e["direccion_municipio"],
                          "complemento": e["direccion_complemento"]},
            "telefono": e["telefono"], "correo": e["correo"],
            "codEstable": e.get("codigo_establecimiento", "M001"),
            "codPuntoVenta": e.get("codigo_punto_venta", "P001"),
        }
        otros = cd_p.get("otros_documentos", [{"codigoDocumento": "01",
                 "descDocumento": "Acta de donacion", "detalleDocumento": "Acta notarial"}])
        dte = self._ident(kw, "15")
        dte["donante"] = donante
        dte["donatario"] = donatario
        dte["otrosDocumentos"] = otros
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["extension"] = kw.get("extension") or {
            "nombEntrega": e["nombre"], "docuEntrega": e["nit"], "codEmpleado": None}
        dte["apendice"] = None
        return dte

    # === HELPERS ===
    def _ident(self, kw, tipo_dte="01") -> dict:
        return {"identificacion": {
            "version": kw["version"], "ambiente": self.ambiente,
            "tipoDte": tipo_dte,
            "numeroControl": kw["numero_control"],
            "codigoGeneracion": kw["codigo_generacion"],
            "tipoModelo": 1, "tipoOperacion": 1,
            "tipoContingencia": None, "motivoContin": None,
            "fecEmi": _now_date(), "horEmi": _now_time(), "tipoMoneda": "USD"}}

    def _wrap_std(self, tipo: str, kw: dict, receptor: dict,
                  cuerpo: list, resumen: dict) -> dict:
        dte = self._ident(kw)
        dte["identificacion"]["tipoDte"] = tipo
        dte["documentoRelacionado"] = None
        dte["emisor"] = self._emisor_std()
        dte["receptor"] = receptor
        dte["otrosDocumentos"] = None
        dte["ventaTercero"] = None
        dte["cuerpoDocumento"] = cuerpo
        dte["resumen"] = resumen
        dte["extension"] = kw.get("extension")
        dte["apendice"] = None
        return dte

    def _emisor_std(self) -> dict:
        e = self.emisor
        return {
            "nit": e["nit"], "nrc": e["nrc"], "nombre": e["nombre"],
            "codActividad": e["cod_actividad"], "descActividad": e["desc_actividad"],
            "nombreComercial": e.get("nombre_comercial"),
            "tipoEstablecimiento": e.get("tipo_establecimiento", "01"),
            "direccion": {"departamento": e["direccion_departamento"],
                          "municipio": e["direccion_municipio"],
                          "complemento": e["direccion_complemento"]},
            "telefono": e["telefono"], "correo": e["correo"],
            "codEstableMH": e.get("codigo_establecimiento", "M001"),
            "codEstable": e.get("codigo_establecimiento", "M001"),
            "codPuntoVentaMH": e.get("codigo_punto_venta", "P001"),
            "codPuntoVenta": e.get("codigo_punto_venta", "P001"),
        }

    def _rec_factura(self, r: dict) -> dict:
        return {
            "tipoDocumento": r.get("tipo_documento", "36"),
            "numDocumento": r.get("num_documento"),
            "nrc": r.get("nrc"), "nombre": r["nombre"],
            "codActividad": r.get("cod_actividad"),
            "descActividad": r.get("desc_actividad"),
            "direccion": {"departamento": r.get("direccion_departamento", "06"),
                          "municipio": r.get("direccion_municipio", "14"),
                          "complemento": r.get("direccion_complemento", "San Salvador")},
            "telefono": r.get("telefono"), "correo": r.get("correo"),
        }

    def _rec_ccf(self, r: dict) -> dict:
        return {
            "nit": r.get("nit") or r.get("num_documento"),
            "nrc": r.get("nrc"), "nombre": r["nombre"],
            "codActividad": r.get("cod_actividad"),
            "descActividad": r.get("desc_actividad"),
            "nombreComercial": r.get("nombre_comercial"),
            "direccion": {"departamento": r.get("direccion_departamento", "06"),
                          "municipio": r.get("direccion_municipio", "14"),
                          "complemento": r.get("direccion_complemento", "San Salvador")},
            "telefono": r.get("telefono"), "correo": r.get("correo"),
        }

    @staticmethod
    def _monto_letras(monto: float) -> str:
        entero = int(monto)
        centavos = int(round((monto - entero) * 100))
        unidades = ["", "UN", "DOS", "TRES", "CUATRO", "CINCO",
                     "SEIS", "SIETE", "OCHO", "NUEVE"]
        decenas = ["", "DIEZ", "VEINTE", "TREINTA", "CUARENTA", "CINCUENTA",
                    "SESENTA", "SETENTA", "OCHENTA", "NOVENTA"]
        especiales = {11: "ONCE", 12: "DOCE", 13: "TRECE", 14: "CATORCE",
                      15: "QUINCE", 16: "DIECISEIS", 17: "DIECISIETE",
                      18: "DIECIOCHO", 19: "DIECINUEVE"}
        def _n(n):
            if n == 0: return "CERO"
            if n < 10: return unidades[n]
            if n in especiales: return especiales[n]
            if n < 20: return f"DIECI{unidades[n-10]}"
            if n < 100:
                d, u = divmod(n, 10)
                if n == 21: return "VEINTIUN"
                if 21 < n < 30: return f"VEINTI{unidades[u]}"
                return f"{decenas[d]} Y {unidades[u]}" if u else decenas[d]
            if n < 1000:
                c, r = divmod(n, 100)
                if n == 100: return "CIEN"
                p = "CIENTO" if c == 1 else "QUINIENTOS" if c == 5 else "SETECIENTOS" if c == 7 else "NOVECIENTOS" if c == 9 else f"{unidades[c]}CIENTOS"
                return f"{p} {_n(r)}" if r else p
            if n < 1000000:
                m, r = divmod(n, 1000)
                p = "MIL" if m == 1 else f"{_n(m)} MIL"
                return f"{p} {_n(r)}" if r else p
            return str(n)
        return f"{_n(entero)} {centavos:02d}/100 DOLARES"
