"""
FACTURA-SV: Test Suite — DTEBuilder vs Certified MH JSONs
=========================================================
Validates that every DTE type produces output structurally
identical to the JSONs that were PROCESADO during certification.

Run: python -m pytest tests/test_dte_builder_certified.py -v
"""
import pytest
import json
from app.mh.dte_builder import DTEBuilder, DTE_VERSIONS

# Standard emisor matching certified data
EMISOR = {
    "nit": "06141212711033", "nrc": "1549809",
    "nombre": "HUGO ERNESTO VARGAS OLIVA",
    "cod_actividad": "58200",
    "desc_actividad": "Edicion de programas informaticos",
    "nombre_comercial": "EFFICIENT AI ALGORITHMS",
    "tipo_establecimiento": "01",
    "direccion_departamento": "06", "direccion_municipio": "14",
    "direccion_complemento": "San Salvador, El Salvador",
    "direccion_distrito": "01",
    "telefono": "00000000", "correo": "hugovargas2003@gmail.com",
    "codigo_establecimiento": "M001", "codigo_punto_venta": "P001",
}

# Standard receptors
REC_CCF = {
    "nit": "06140711071030", "nrc": "1832035",
    "nombre": "OD EL SALVADOR LTDA, DE C.V.",
    "cod_actividad": "46592",
    "desc_actividad": "Venta al por mayor de maquinaria",
    "nombre_comercial": "OD EL SALVADOR LTDA, DE C.V.",
    "direccion_departamento": "06", "direccion_municipio": "23",
    "direccion_complemento": "PASEO GENERAL ESCALON",
    "telefono": "22604050", "correo": "edith.fernandez@officedepot.com.sv",
}

REC_FACTURA = {
    "tipo_documento": "36", "num_documento": "00000000000000",
    "nombre": "Consumidor Final",
    "direccion_departamento": "06", "direccion_municipio": "14",
    "direccion_complemento": "San Salvador, El Salvador",
    "telefono": "00000000", "correo": "consumidor@test.com",
}

ITEMS_STD = [{"precio_unitario": 100.0, "descripcion": "Servicio de prueba",
              "codigo": "SERV001", "tipo_item": 1, "cantidad": 1}]

ITEMS_RET = [{"monto_sujeto": 500.0, "iva_retenido": 5.0,
              "tipo_dte_ref": "03", "tipo_generacion": 1,
              "num_documento": "00010001000000001",
              "descripcion": "Retencion IVA 1%", "codigo_retencion": "22"}]

ITEMS_DON = [{"descripcion": "Computadoras donadas", "valor": 1000.0,
              "codigo": "DON001", "tipo_item": 1}]


def make_builder(ambiente="00"):
    return DTEBuilder(EMISOR, ambiente)


class TestVersions:
    """Verify DTE_VERSIONS matches certification."""
    CERTIFIED = {"01": 1, "03": 3, "04": 3, "05": 3, "06": 3,
                 "07": 2, "08": 1, "09": 1, "11": 1, "14": 1, "15": 1}

    def test_all_versions(self):
        for tipo, expected in self.CERTIFIED.items():
            assert DTE_VERSIONS[tipo] == expected, \
                f"Tipo {tipo}: expected v{expected}, got v{DTE_VERSIONS[tipo]}"


class TestFactura01:
    """Factura — version 1, ivaItem extracted, montoTotal=totalGravada."""

    def setup_method(self):
        b = make_builder()
        self.dte, self.cg = b.build("01", "DTE-01-M001P001-000000000000001",
                                     REC_FACTURA, ITEMS_STD)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 1

    def test_top_level_keys(self):
        expected = ["identificacion", "documentoRelacionado", "emisor",
                    "receptor", "otrosDocumentos", "ventaTercero",
                    "cuerpoDocumento", "resumen", "extension", "apendice"]
        assert list(self.dte.keys()) == expected

    def test_iva_item_extraction(self):
        """ivaItem = ventaGravada - ventaGravada/1.13 (NOT * 0.13)"""
        item = self.dte["cuerpoDocumento"][0]
        vg = item["ventaGravada"]
        expected_iva = round(vg - vg / 1.13, 2)
        assert item["ivaItem"] == expected_iva
        # 100 - 100/1.13 = 11.50 (not 13.00)
        assert item["ivaItem"] == 11.5

    def test_monto_total_equals_gravada(self):
        """In Factura, montoTotalOperacion = totalGravada (IVA inclusive)."""
        r = self.dte["resumen"]
        assert r["montoTotalOperacion"] == r["totalGravada"]
        assert r["totalPagar"] == r["totalGravada"]

    def test_resumen_tributos_null(self):
        assert self.dte["resumen"]["tributos"] is None

    def test_resumen_has_totalIva(self):
        assert "totalIva" in self.dte["resumen"]
        assert self.dte["resumen"]["totalIva"] > 0

    def test_ivaRete1_zero(self):
        assert self.dte["resumen"]["ivaRete1"] == 0.0

    def test_item_tributos_null(self):
        assert self.dte["cuerpoDocumento"][0]["tributos"] is None

    def test_item_has_ivaItem(self):
        assert "ivaItem" in self.dte["cuerpoDocumento"][0]

    def test_pagos_referencia_empty_string(self):
        assert self.dte["resumen"]["pagos"][0]["referencia"] == ""

    def test_saldoFavor_present(self):
        assert "saldoFavor" in self.dte["resumen"]


class TestCCF03:
    """CCF — version 3, IVA separate, tributos in items."""

    def setup_method(self):
        b = make_builder()
        self.dte, _ = b.build("03", "DTE-03-M001P001-000000000000001",
                               REC_CCF, ITEMS_STD)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 3

    def test_item_has_tributos_20(self):
        assert self.dte["cuerpoDocumento"][0]["tributos"] == ["20"]

    def test_item_no_ivaItem(self):
        assert "ivaItem" not in self.dte["cuerpoDocumento"][0]

    def test_resumen_tributos_array(self):
        t = self.dte["resumen"]["tributos"]
        assert isinstance(t, list) and len(t) == 1
        assert t[0]["codigo"] == "20"

    def test_resumen_has_ivaPerci1(self):
        assert self.dte["resumen"]["ivaPerci1"] == 0.0

    def test_resumen_has_saldoFavor(self):
        assert self.dte["resumen"]["saldoFavor"] == 0.0

    def test_monto_includes_iva(self):
        r = self.dte["resumen"]
        assert r["montoTotalOperacion"] == round(r["totalGravada"] * 1.13, 2)

    def test_receptor_has_nit(self):
        assert "nit" in self.dte["receptor"]
        assert "tipoDocumento" not in self.dte["receptor"]

    def test_no_totalIva(self):
        assert "totalIva" not in self.dte["resumen"]

    def test_item_has_psv_noGravado(self):
        item = self.dte["cuerpoDocumento"][0]
        assert item["psv"] == 0.0
        assert item["noGravado"] == 0.0


class TestNR04:
    """Nota de Remisión — bienTitulo required, full resumen."""

    def setup_method(self):
        rec = {**REC_CCF, "tipo_documento": "36",
               "num_documento": "06141212711033", "bien_titulo": "04"}
        b = make_builder()
        self.dte, _ = b.build("04", "DTE-04-M001P001-000000000000001",
                               rec, ITEMS_STD)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 3

    def test_receptor_has_bienTitulo(self):
        assert self.dte["receptor"]["bienTitulo"] == "04"

    def test_receptor_uses_tipoDocumento(self):
        assert "tipoDocumento" in self.dte["receptor"]
        assert "nit" not in self.dte["receptor"]

    def test_items_have_tributos(self):
        assert self.dte["cuerpoDocumento"][0]["tributos"] == ["20"]

    def test_resumen_complete(self):
        r = self.dte["resumen"]
        for key in ["totalGravada", "tributos", "ivaPerci1",
                     "montoTotalOperacion", "pagos"]:
            assert key in r, f"Missing {key} in NR resumen"

    def test_no_otrosDocumentos(self):
        assert "otrosDocumentos" not in self.dte


class TestNC05:
    """Nota de Crédito — prohibited fields, emisor without codEstableMH."""

    def setup_method(self):
        b = make_builder()
        ref = {"tipo_dte": "03", "codigo_generacion": "FAKE-UUID-1234",
               "fecha_emision": "2026-02-20"}
        self.dte, _ = b.build("05", "DTE-05-M001P001-000000000000001",
                               REC_CCF, ITEMS_STD, dte_referencia=ref)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 3

    def test_prohibited_fields_absent(self):
        """NC must NOT have pagos, totalPagar, saldoFavor, etc."""
        r = self.dte["resumen"]
        prohibited = ["pagos", "numPagoElectronico", "porcentajeDescuento",
                       "totalNoGravado", "saldoFavor", "totalPagar"]
        for field in prohibited:
            assert field not in r, f"Prohibited field '{field}' in NC resumen"

    def test_emisor_no_codEstableMH(self):
        e = self.dte["emisor"]
        assert "codEstableMH" not in e
        assert "codEstable" not in e
        assert "codPuntoVentaMH" not in e

    def test_documentoRelacionado(self):
        dr = self.dte["documentoRelacionado"]
        assert isinstance(dr, list) and len(dr) == 1
        assert dr[0]["tipoDocumento"] == "03"

    def test_item_numeroDocumento_matches_ref(self):
        assert self.dte["cuerpoDocumento"][0]["numeroDocumento"] == "FAKE-UUID-1234"


class TestND06:
    """Nota de Débito — similar to NC but with numPagoElectronico."""

    def setup_method(self):
        b = make_builder()
        ref = {"tipo_dte": "03", "codigo_generacion": "FAKE-UUID-5678"}
        self.dte, _ = b.build("06", "DTE-06-M001P001-000000000000001",
                               REC_CCF, ITEMS_STD, dte_referencia=ref)

    def test_has_numPagoElectronico(self):
        assert "numPagoElectronico" in self.dte["resumen"]

    def test_no_pagos(self):
        assert "pagos" not in self.dte["resumen"]

    def test_no_totalPagar(self):
        assert "totalPagar" not in self.dte["resumen"]


class TestCR07:
    """Comprobante de Retención — unique emisor, v2."""

    def setup_method(self):
        b = make_builder()
        self.dte, _ = b.build("07", "DTE-07-M001P001-000000000000001",
                               REC_CCF, ITEMS_RET)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 2

    def test_emisor_has_distrito(self):
        assert "distrito" in self.dte["emisor"]["direccion"]

    def test_emisor_no_tipoEstablecimiento(self):
        assert "tipoEstablecimiento" not in self.dte["emisor"]

    def test_emisor_codEstable_without_MH(self):
        e = self.dte["emisor"]
        assert "codEstable" in e
        assert "codEstableMH" not in e

    def test_receptor_uses_tipoDocumento(self):
        assert "tipoDocumento" in self.dte["receptor"]
        assert "nit" not in self.dte["receptor"]

    def test_resumen_totalIvaRetenido(self):
        """Must be camelCase totalIvaRetenido (not totalIVAretenido)."""
        assert "totalIvaRetenido" in self.dte["resumen"]

    def test_resumen_totalIva_zero(self):
        assert self.dte["resumen"]["totalIva"] == 0.0

    def test_no_documentoRelacionado(self):
        assert "documentoRelacionado" not in self.dte

    def test_no_otrosDocumentos(self):
        assert "otrosDocumentos" not in self.dte


class TestCL08:
    """Comprobante de Liquidación — version 1, simplified items."""

    def setup_method(self):
        b = make_builder()
        ref = {"tipo_dte": "03", "tipo_generacion": 1,
               "num_documento": "00010001000000001"}
        self.dte, _ = b.build("08", "DTE-08-M001P001-000000000000001",
                               REC_CCF, ITEMS_STD, dte_referencia=ref)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 1

    def test_no_totalNoSuj(self):
        assert "totalNoSuj" not in self.dte["resumen"]

    def test_no_totalExenta(self):
        assert "totalExenta" not in self.dte["resumen"]

    def test_no_ivaPerci1(self):
        assert "ivaPerci1" not in self.dte["resumen"]

    def test_items_simplified(self):
        item = self.dte["cuerpoDocumento"][0]
        assert "ventaNoSuj" not in item
        assert "ventaExenta" not in item
        assert "tipoDte" not in item


class TestDCL09:
    """Documento Contable de Liquidación — object body, unique emisor."""

    def setup_method(self):
        b = make_builder()
        self.dte, _ = b.build("09", "DTE-09-M001P001-000000000000001",
                               REC_CCF, ITEMS_STD,
                               dcl_params={"valor_operaciones": 1130.0})

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 1

    def test_cuerpo_is_object(self):
        assert isinstance(self.dte["cuerpoDocumento"], dict)

    def test_emisor_codigoMH(self):
        e = self.dte["emisor"]
        assert "codigoMH" in e and "puntoVentaMH" in e
        assert "codEstableMH" not in e

    def test_porcentComision_integer(self):
        assert isinstance(self.dte["cuerpoDocumento"]["porcentComision"], int)

    def test_receptor_has_extra_fields(self):
        r = self.dte["receptor"]
        assert "tipoEstablecimiento" in r
        assert "codigoMH" in r


class TestFEXE11:
    """Factura de Exportación — motivoContigencia, tipoItemExpor."""

    def setup_method(self):
        b = make_builder()
        rec = {"nombre": "Foreign Client", "tipo_documento": "37",
               "num_documento": "000000000", "cod_pais": "9300",
               "nombre_pais": "ESTADOS UNIDOS", "correo": "x@y.com"}
        self.dte, _ = b.build("11", "DTE-11-M001P001-000000000000001",
                               rec, ITEMS_STD)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 1

    def test_motivoContigencia_spelling(self):
        ident = self.dte["identificacion"]
        assert "motivoContigencia" in ident
        assert "motivoContin" not in ident

    def test_emisor_tipoItemExpor(self):
        e = self.dte["emisor"]
        assert e["tipoItemExpor"] == 1
        assert "recintoFiscal" in e
        assert "regimen" in e

    def test_receptor_codPais(self):
        r = self.dte["receptor"]
        assert r["codPais"] == "9300"
        assert "nombrePais" in r


class TestFSE14:
    """Factura Sujeto Excluido — sujetoExcluido, no receptor."""

    def setup_method(self):
        b = make_builder()
        rec = {"nombre": "Juan Perez", "tipo_documento": "13",
               "num_documento": "000000000", "cod_actividad": "47190",
               "desc_actividad": "Venta al por menor",
               "telefono": "70001234", "correo": "x@test.com"}
        self.dte, _ = b.build("14", "DTE-14-M001P001-000000000000001",
                               rec, ITEMS_STD)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 1

    def test_uses_sujetoExcluido(self):
        assert "sujetoExcluido" in self.dte
        assert "receptor" not in self.dte

    def test_emisor_no_nombreComercial(self):
        assert "nombreComercial" not in self.dte["emisor"]

    def test_emisor_no_tipoEstablecimiento(self):
        assert "tipoEstablecimiento" not in self.dte["emisor"]

    def test_resumen_has_observaciones(self):
        assert "observaciones" in self.dte["resumen"]

    def test_item_has_compra(self):
        assert "compra" in self.dte["cuerpoDocumento"][0]
        assert "ventaGravada" not in self.dte["cuerpoDocumento"][0]


class TestCD15:
    """Comprobante de Donación — donante/donatario, otrosDocumentos."""

    def setup_method(self):
        b = make_builder()
        rec = {"nombre": "OD EL SALVADOR", "tipo_documento": "36",
               "nit": "06140711071030", "nrc": "1832035",
               "cod_actividad": "46592", "desc_actividad": "Venta",
               "telefono": "22604050", "correo": "ed@od.com",
               "cod_pais": "9300"}
        self.dte, _ = b.build("15", "DTE-15-M001P001-000000000000001",
                               rec, ITEMS_DON)

    def test_version(self):
        assert self.dte["identificacion"]["version"] == 1

    def test_uses_donante_donatario(self):
        assert "donante" in self.dte
        assert "donatario" in self.dte
        assert "emisor" not in self.dte
        assert "receptor" not in self.dte

    def test_donante_has_codPais(self):
        assert self.dte["donante"]["codPais"] == "9300"

    def test_otrosDocumentos_array(self):
        od = self.dte["otrosDocumentos"]
        assert isinstance(od, list) and len(od) >= 1
        assert "codigoDocumento" in od[0]

    def test_item_has_valorDonacion(self):
        item = self.dte["cuerpoDocumento"][0]
        assert "valorDonacion" in item
        assert item["uniMedida"] == 99

    def test_resumen_totalDonacion(self):
        assert "totalDonacion" in self.dte["resumen"]
        assert "valorTotal" not in self.dte["resumen"]

    def test_donatario_has_codEstable(self):
        d = self.dte["donatario"]
        assert "codEstable" in d
        assert "codPuntoVenta" in d


class TestSignEngine:
    """Verify sign_engine uses RS512/JWS (not PyJWT RS256)."""

    def test_jws_header(self):
        from app.modules.sign_engine import _JWS_HEADER
        assert _JWS_HEADER == {"alg": "RS512", "typ": "JWS"}

    def test_sign_with_pem_produces_jws(self):
        """Verify JWS structure: 3 parts, correct header."""
        from app.modules.sign_engine import SignEngine, _b64url_decode
        import json as j
        # Generate a test key
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        pk = rsa.generate_private_key(65537, 2048)
        pem = pk.private_bytes(serialization.Encoding.PEM,
                               serialization.PrivateFormat.PKCS8,
                               serialization.NoEncryption()).decode()
        jws = SignEngine.sign_with_pem(pem, {"test": "data"})
        parts = jws.split(".")
        assert len(parts) == 3
        header = j.loads(_b64url_decode(parts[0]))
        assert header == {"alg": "RS512", "typ": "JWS"}
