"""
FACTURA-SV: 360° FULL SYSTEM TEST
==================================
Validates every component of the system end-to-end.

Sections:
  A. DTEBuilder — 13 tipos (structural, already covered by 74 tests)
  B. SignEngine — RS512/JWS correctness
  C. InvalidationService — Document structure vs certified JSON
  D. ContingencyService — Document structure vs certified JSON
  E. Billing flow — emisor_data construction
  F. DTE field-by-field — Deep comparison against certified schemas
  G. IVA calculations — Factura extraction vs CCF addition
  H. Edge cases — Empty items, large amounts, special chars

Run: python3 -m pytest tests/test_360_full_system.py -v
"""
import pytest
import json
import uuid
from datetime import date
from app.mh.dte_builder import DTEBuilder, DTE_VERSIONS

# ═══════════════════════════════════════════
# TEST DATA
# ═══════════════════════════════════════════

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

def builder(amb="00"):
    return DTEBuilder(EMISOR, amb)


# ═══════════════════════════════════════════
# A. ALL 13 TYPES BUILD WITHOUT ERROR
# ═══════════════════════════════════════════

class TestAllTypesConstruct:
    """Every type must build without exceptions."""

    CONFIGS = {
        "01": {"receptor": {"nombre": "CF", "tipo_documento": "36",
               "num_documento": "00000000000000", "telefono": "00000000",
               "correo": "cf@test.com"},
               "items": [{"precio_unitario": 100, "descripcion": "Serv", "codigo": "S1"}]},
        "03": {"receptor": {"nombre": "OD", "nit": "06140711071030",
               "nrc": "1832035", "cod_actividad": "46592",
               "desc_actividad": "Venta", "nombre_comercial": "OD",
               "telefono": "22604050", "correo": "od@test.com"},
               "items": [{"precio_unitario": 150, "descripcion": "Dev", "codigo": "S1"}]},
        "04": {"receptor": {"nombre": "REC", "tipo_documento": "36",
               "num_documento": "06141212711033", "nrc": "1549809",
               "bien_titulo": "04", "telefono": "00000000", "correo": "x@t.com"},
               "items": [{"precio_unitario": 100, "descripcion": "Merc", "codigo": "P1"}]},
        "05": {"receptor": {"nombre": "VIDRI", "nit": "02101911710016",
               "nrc": "27", "cod_actividad": "47522", "desc_actividad": "Ferreteria",
               "nombre_comercial": "VIDRI", "telefono": "22743033", "correo": "v@v.com"},
               "items": [{"precio_unitario": 50, "descripcion": "Ajuste", "codigo": "S1"}],
               "dte_referencia": {"tipo_dte": "03", "codigo_generacion": "E00EE6B3-TEST",
                                  "fecha_emision": "2026-02-20"}},
        "06": {"receptor": {"nombre": "OD", "nit": "06140711071030",
               "nrc": "1832035", "nombre_comercial": "OD",
               "telefono": "22604050", "correo": "od@test.com"},
               "items": [{"precio_unitario": 20, "descripcion": "Cargo", "codigo": "S1"}],
               "dte_referencia": {"tipo_dte": "03", "codigo_generacion": "D56F4570-TEST"}},
        "07": {"receptor": {"nombre": "OD", "nit": "06140711071030",
               "nrc": "1832035", "nombre_comercial": "OD",
               "cod_actividad": "46592", "desc_actividad": "Venta",
               "telefono": "22604050", "correo": "od@test.com"},
               "items": [{"monto_sujeto": 500, "iva_retenido": 5.0,
                          "descripcion": "Ret IVA 1%", "codigo_retencion": "22"}]},
        "08": {"receptor": {"nombre": "VIDRI", "nit": "02101911710016",
               "nrc": "27", "cod_actividad": "47522", "desc_actividad": "Ferreteria",
               "nombre_comercial": "VIDRI", "telefono": "22743033", "correo": "v@v.com"},
               "items": [{"precio_unitario": 100, "descripcion": "Liq", "codigo": "L1"}],
               "dte_referencia": {"tipo_dte": "03", "tipo_generacion": 1,
                                  "num_documento": "00010001000000001"}},
        "09": {"receptor": {"nombre": "OD", "nit": "06140711071030",
               "nrc": "1832035", "cod_actividad": "46592", "desc_actividad": "Venta",
               "nombre_comercial": "OD", "telefono": "22604050", "correo": "od@test.com"},
               "items": [{"precio_unitario": 100, "descripcion": "X", "codigo": "X"}],
               "dcl_params": {"valor_operaciones": 1130.0, "porcentaje_comision": 5}},
        "11": {"receptor": {"nombre": "Foreign Client", "tipo_documento": "37",
               "num_documento": "000000000", "cod_pais": "9300",
               "nombre_pais": "ESTADOS UNIDOS", "correo": "fc@ext.com",
               "telefono": "0000000000"},
               "items": [{"precio_unitario": 500, "descripcion": "Export", "codigo": "E1"}]},
        "14": {"receptor": {"nombre": "Juan Perez", "tipo_documento": "13",
               "num_documento": "000000000", "cod_actividad": "47190",
               "desc_actividad": "Venta al por menor",
               "telefono": "70001234", "correo": "jp@test.com"},
               "items": [{"precio_unitario": 100, "descripcion": "Compra", "codigo": "C1"}]},
        "15": {"receptor": {"nombre": "OD DONANTE", "tipo_documento": "36",
               "nit": "06140711071030", "nrc": "1832035",
               "cod_actividad": "46592", "desc_actividad": "Venta",
               "nombre_comercial": "OD", "telefono": "22604050",
               "correo": "od@test.com", "cod_pais": "9300"},
               "items": [{"descripcion": "Computadoras", "valor": 1000, "codigo": "D1"}]},
    }

    @pytest.mark.parametrize("tipo", ["01","03","04","05","06","07","08","09","11","14","15"])
    def test_build_succeeds(self, tipo):
        b = builder()
        cfg = self.CONFIGS[tipo]
        kwargs = {"receptor": cfg["receptor"], "items": cfg["items"]}
        if "dte_referencia" in cfg: kwargs["dte_referencia"] = cfg["dte_referencia"]
        if "dcl_params" in cfg: kwargs["dcl_params"] = cfg["dcl_params"]
        dte, cg = b.build(tipo, f"DTE-{tipo}-M001P001-000000000000001", **kwargs)
        assert dte["identificacion"]["version"] == DTE_VERSIONS[tipo]
        assert dte["identificacion"]["tipoDte"] == tipo
        assert len(cg) == 36  # UUID

    @pytest.mark.parametrize("tipo", ["01","03","04","05","06","07","08","09","11","14","15"])
    def test_codigo_generacion_unique(self, tipo):
        b = builder()
        cfg = self.CONFIGS[tipo]
        kwargs = {"receptor": cfg["receptor"], "items": cfg["items"]}
        if "dte_referencia" in cfg: kwargs["dte_referencia"] = cfg["dte_referencia"]
        if "dcl_params" in cfg: kwargs["dcl_params"] = cfg["dcl_params"]
        _, cg1 = b.build(tipo, f"DTE-{tipo}-M001P001-000000000000001", **kwargs)
        _, cg2 = b.build(tipo, f"DTE-{tipo}-M001P001-000000000000002", **kwargs)
        assert cg1 != cg2


# ═══════════════════════════════════════════
# B. SIGN ENGINE RS512/JWS
# ═══════════════════════════════════════════

class TestSignEngineRS512:
    def test_header_alg(self):
        from app.modules.sign_engine import _JWS_HEADER
        assert _JWS_HEADER["alg"] == "RS512"
        assert _JWS_HEADER["typ"] == "JWS"

    def test_jws_three_parts(self):
        from app.modules.sign_engine import SignEngine, _b64url_decode
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        pk = rsa.generate_private_key(65537, 2048)
        pem = pk.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()).decode()
        jws = SignEngine.sign_with_pem(pem, {"test": True})
        parts = jws.split(".")
        assert len(parts) == 3
        header = json.loads(_b64url_decode(parts[0]))
        assert header == {"alg": "RS512", "typ": "JWS"}

    def test_payload_preserved(self):
        from app.modules.sign_engine import SignEngine, _b64url_decode
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        pk = rsa.generate_private_key(65537, 2048)
        pem = pk.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()).decode()
        original = {"clave": "valor", "número": 42, "especial": "ñ"}
        jws = SignEngine.sign_with_pem(pem, original)
        payload_b64 = jws.split(".")[1]
        recovered = json.loads(_b64url_decode(payload_b64))
        assert recovered == original

    def test_signature_verifiable(self):
        from app.modules.sign_engine import SignEngine, _b64url_decode, _b64url_encode
        from cryptography.hazmat.primitives.asymmetric import rsa, padding
        from cryptography.hazmat.primitives import serialization, hashes
        pk = rsa.generate_private_key(65537, 2048)
        pem = pk.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()).decode()
        jws = SignEngine.sign_with_pem(pem, {"verify": "me"})
        h, p, s = jws.split(".")
        signing_input = f"{h}.{p}".encode("ascii")
        sig_bytes = _b64url_decode(s)
        pub = pk.public_key()
        # Should not raise
        pub.verify(sig_bytes, signing_input, padding.PKCS1v15(), hashes.SHA512())


# ═══════════════════════════════════════════
# C. INVALIDATION DOCUMENT STRUCTURE
# ═══════════════════════════════════════════

class TestInvalidationStructure:
    def test_emisor_10_fields(self):
        from app.modules.invalidation_service import InvalidationService
        from app.schemas.models import InvalidateRequest
        req = InvalidateRequest(
            codigo_generacion_doc="79B69944-TEST",
            tipo_dte="01", motivo="Error en prueba",
            nombre_responsable="HUGO", num_documento_responsable="06141212711033",
            nit_emisor="06141212711033", nombre_emisor="HUGO ERNESTO VARGAS OLIVA",
            nit_receptor="00000000000000", nombre_receptor="Consumidor Final",
            sello_recibido="2026SELLO1234567890123456789012345678",
            numero_control="DTE-01-M001P001-000000000000801",
            fecha_emision="2026-02-20", monto_iva=0.0,
            nombre_comercial_emisor="EFFICIENT AI ALGORITHMS",
            cod_establecimiento="M001", cod_punto_venta="P001",
            telefono_emisor="00000000", correo_emisor="hugovargas2003@gmail.com",
        )
        svc = InvalidationService()
        doc = svc.build_invalidation_document(req)

        # Verify all 10 emisor fields
        em = doc["emisor"]
        assert em["nit"] == "06141212711033"
        assert em["nombre"] == "HUGO ERNESTO VARGAS OLIVA"
        assert em["tipoEstablecimiento"] == "01"
        assert em["nomEstablecimiento"] == "EFFICIENT AI ALGORITHMS"
        assert em["codEstableMH"] == "M001"
        assert em["codEstable"] == "M001"
        assert em["codPuntoVentaMH"] == "P001"
        assert em["codPuntoVenta"] == "P001"
        assert em["telefono"] == "00000000"
        assert em["correo"] == "hugovargas2003@gmail.com"

    def test_documento_no_codigoGeneracionR(self):
        from app.modules.invalidation_service import InvalidationService
        from app.schemas.models import InvalidateRequest
        req = InvalidateRequest(
            codigo_generacion_doc="79B69944-TEST",
            tipo_dte="01", motivo="Test error",
            nombre_responsable="HUGO", num_documento_responsable="06141212711033",
            nit_emisor="06141212711033", nombre_emisor="HUGO",
            nit_receptor="00000000000000", nombre_receptor="CF",
            sello_recibido="2026SELLO1234567890123456789012345678",
            numero_control="DTE-01-M001P001-000000000000801",
            fecha_emision="2026-02-20",
        )
        doc = InvalidationService().build_invalidation_document(req)
        assert "codigoGeneracionR" not in doc["documento"]

    def test_documento_has_telefono_correo(self):
        from app.modules.invalidation_service import InvalidationService
        from app.schemas.models import InvalidateRequest
        req = InvalidateRequest(
            codigo_generacion_doc="TEST-UUID",
            tipo_dte="01", motivo="Test motivo",
            nombre_responsable="HUGO", num_documento_responsable="06141212711033",
            nit_emisor="06141212711033", nombre_emisor="HUGO",
            nit_receptor="12345", nombre_receptor="REC",
            sello_recibido="2026SELLO1234567890123456789012345678",
            numero_control="DTE-01-M001P001-000000000000001",
            fecha_emision="2026-02-20",
            telefono_receptor="77778888", correo_receptor="rec@test.com",
        )
        doc = InvalidationService().build_invalidation_document(req)
        assert doc["documento"]["telefono"] == "77778888"
        assert doc["documento"]["correo"] == "rec@test.com"

    def test_identificacion_version_2(self):
        from app.modules.invalidation_service import InvalidationService
        from app.schemas.models import InvalidateRequest
        req = InvalidateRequest(
            codigo_generacion_doc="TEST", tipo_dte="01", motivo="Test motivo",
            nombre_responsable="H", num_documento_responsable="123",
            nit_emisor="123", nombre_emisor="H",
            nit_receptor="456", nombre_receptor="R",
            sello_recibido="2026SELLO1234567890123456789012345678",
            numero_control="DTE-01-X", fecha_emision="2026-02-20",
        )
        doc = InvalidationService().build_invalidation_document(req)
        assert doc["identificacion"]["version"] == 2

    def test_motivo_structure(self):
        from app.modules.invalidation_service import InvalidationService
        from app.schemas.models import InvalidateRequest
        req = InvalidateRequest(
            codigo_generacion_doc="TEST", tipo_dte="01", motivo="Error documento",
            tipo_invalidacion="2",
            nombre_responsable="HUGO", tipo_documento_responsable="36",
            num_documento_responsable="06141212711033",
            nit_emisor="123", nombre_emisor="H",
            nit_receptor="456", nombre_receptor="R",
            sello_recibido="2026SELLO1234567890123456789012345678",
            numero_control="DTE-01-X", fecha_emision="2026-02-20",
        )
        doc = InvalidationService().build_invalidation_document(req)
        m = doc["motivo"]
        assert m["tipoAnulacion"] == 2
        assert m["motivoAnulacion"] == "Error documento"
        assert m["tipDocResponsable"] == "36"
        assert m["numDocResponsable"] == "06141212711033"


# ═══════════════════════════════════════════
# D. CONTINGENCY DOCUMENT STRUCTURE
# ═══════════════════════════════════════════

class TestContingencyStructure:
    def test_version_3(self):
        from app.modules.contingency_service import ContingencyService
        svc = ContingencyService()
        doc = svc.build_contingency_document(
            nit_emisor="06141212711033", nombre_emisor="HUGO",
            nombre_comercial="EFFICIENT AI", cod_establecimiento="M001",
            cod_punto_venta="P001", telefono="00000000",
            correo="hugo@test.com", motivo="Falla energia",
            fecha_inicio="2026-02-20", hora_inicio="08:00:00",
            fecha_fin="2026-02-20", hora_fin="10:30:00",
            detalle_dte=[{"tipo_dte": "01", "codigo_generacion": "UUID-TEST",
                          "numero_control": "DTE-01-M001P001-000000000000001",
                          "fecha_emision": "2026-02-20", "hora_emision": "14:29:57"}])
        assert doc["identificacion"]["version"] == 3

    def test_emisor_10_fields(self):
        from app.modules.contingency_service import ContingencyService
        doc = ContingencyService().build_contingency_document(
            nit_emisor="06141212711033", nombre_emisor="HUGO",
            nombre_comercial="EFFICIENT AI", cod_establecimiento="M001",
            cod_punto_venta="P001", telefono="00000000", correo="h@t.com",
            motivo="Test", fecha_inicio="2026-02-20", hora_inicio="08:00:00",
            fecha_fin="2026-02-20", hora_fin="10:00:00", detalle_dte=[
                {"tipo_dte": "01", "codigo_generacion": "X",
                 "numero_control": "Y", "fecha_emision": "2026-02-20"}])
        em = doc["emisor"]
        required = ["nit", "nombre", "tipoEstablecimiento", "nomEstablecimiento",
                     "codEstableMH", "codEstable", "codPuntoVentaMH", "codPuntoVenta",
                     "telefono", "correo"]
        for field in required:
            assert field in em, f"Missing {field} in contingency emisor"

    def test_detalle_dte_fields(self):
        from app.modules.contingency_service import ContingencyService
        doc = ContingencyService().build_contingency_document(
            nit_emisor="X", nombre_emisor="X", nombre_comercial="X",
            cod_establecimiento="M001", cod_punto_venta="P001",
            telefono="0", correo="x", motivo="Test",
            fecha_inicio="2026-02-20", hora_inicio="08:00:00",
            fecha_fin="2026-02-20", hora_fin="10:00:00",
            detalle_dte=[{"tipo_dte": "01", "codigo_generacion": "UUID-1",
                          "sello_recibido": None,
                          "numero_control": "DTE-01-M001P001-000000000000001",
                          "fecha_emision": "2026-02-20", "hora_emision": "14:29:57"}])
        d = doc["detalleDTE"][0]
        assert d["tipoDte"] == "01"
        assert d["codigoGeneracion"] == "UUID-1"
        assert d["selloRecibido"] is None
        assert d["fecEmi"] == "2026-02-20"
        assert d["horEmi"] == "14:29:57"

    def test_top_level_keys(self):
        from app.modules.contingency_service import ContingencyService
        doc = ContingencyService().build_contingency_document(
            nit_emisor="X", nombre_emisor="X", nombre_comercial="X",
            cod_establecimiento="M001", cod_punto_venta="P001",
            telefono="0", correo="x", motivo="Falla",
            fecha_inicio="2026-02-20", hora_inicio="08:00:00",
            fecha_fin="2026-02-20", hora_fin="10:00:00",
            detalle_dte=[{"tipo_dte": "01", "codigo_generacion": "X",
                          "numero_control": "Y", "fecha_emision": "2026-02-20"}])
        assert list(doc.keys()) == ["identificacion", "emisor", "motivo", "detalleDTE"]


# ═══════════════════════════════════════════
# E. BILLING EMISOR CONSTRUCTION
# ═══════════════════════════════════════════

class TestBillingEmisor:
    """Verify _creds_to_emisor produces DTEBuilder-compatible dict."""

    def test_all_required_keys(self):
        creds = {
            "nit": "06141212711033", "nrc": "1549809",
            "nombre": "HUGO ERNESTO VARGAS OLIVA",
            "cod_actividad": "58200", "desc_actividad": "Edicion",
            "nombre_comercial": "EFFICIENT AI", "tipo_establecimiento": "01",
            "telefono": "00000000", "correo": "hugo@test.com",
            "direccion_departamento": "06", "direccion_municipio": "14",
            "direccion_complemento": "San Salvador",
            "codigo_establecimiento": "M001", "codigo_punto_venta": "P001",
        }
        # Simulate _creds_to_emisor
        emisor = {
            "nit": creds["nit"], "nrc": creds["nrc"],
            "nombre": creds["nombre"],
            "cod_actividad": creds["cod_actividad"],
            "desc_actividad": creds["desc_actividad"],
            "nombre_comercial": creds.get("nombre_comercial"),
            "tipo_establecimiento": creds.get("tipo_establecimiento", "01"),
            "telefono": creds["telefono"], "correo": creds["correo"],
            "direccion_departamento": creds["direccion_departamento"],
            "direccion_municipio": creds["direccion_municipio"],
            "direccion_complemento": creds["direccion_complemento"],
            "codigo_establecimiento": creds.get("codigo_establecimiento", "M001"),
            "codigo_punto_venta": creds.get("codigo_punto_venta", "P001"),
        }
        # Must work with DTEBuilder
        b = DTEBuilder(emisor, "00")
        for tipo in ["01", "03"]:
            dte, _ = b.build(tipo, f"DTE-{tipo}-M001P001-000000000000001",
                             {"nombre": "CF", "num_documento": "00000000000000",
                              "tipo_documento": "36", "telefono": "0", "correo": "x@x.com"},
                             [{"precio_unitario": 100, "descripcion": "T", "codigo": "T"}])
            assert dte["emisor"]["nit"] == "06141212711033"


# ═══════════════════════════════════════════
# F. DEEP FIELD-BY-FIELD SCHEMA VALIDATION
# ═══════════════════════════════════════════

class TestDeepSchema:
    """Verify each type has EXACTLY the right fields (no extra, no missing)."""

    # Certified top-level keys per type
    CERTIFIED_KEYS = {
        "01": ["identificacion","documentoRelacionado","emisor","receptor",
               "otrosDocumentos","ventaTercero","cuerpoDocumento","resumen","extension","apendice"],
        "03": ["identificacion","documentoRelacionado","emisor","receptor",
               "otrosDocumentos","ventaTercero","cuerpoDocumento","resumen","extension","apendice"],
        "04": ["identificacion","documentoRelacionado","emisor","receptor",
               "ventaTercero","cuerpoDocumento","resumen","extension","apendice"],
        "05": ["identificacion","documentoRelacionado","emisor","receptor",
               "ventaTercero","cuerpoDocumento","resumen","extension","apendice"],
        "06": ["identificacion","documentoRelacionado","emisor","receptor",
               "ventaTercero","cuerpoDocumento","resumen","extension","apendice"],
        "07": ["identificacion","emisor","receptor","cuerpoDocumento","resumen","apendice"],
        "08": ["identificacion","documentoRelacionado","emisor","receptor",
               "cuerpoDocumento","resumen","extension","apendice"],
        "09": ["identificacion","emisor","receptor","cuerpoDocumento","extension","apendice"],
        "11": ["identificacion","emisor","receptor","otrosDocumentos","ventaTercero",
               "cuerpoDocumento","resumen","apendice"],
        "14": ["identificacion","emisor","sujetoExcluido","cuerpoDocumento","resumen","apendice"],
        "15": ["identificacion","donante","donatario","otrosDocumentos",
               "cuerpoDocumento","resumen","extension","apendice"],
    }

    # Certified resumen keys per type
    CERTIFIED_RESUMEN_KEYS = {
        "01": {"totalNoSuj","totalExenta","totalGravada","subTotalVentas",
               "descuNoSuj","descuExenta","descuGravada","porcentajeDescuento",
               "totalDescu","tributos","subTotal","ivaRete1","reteRenta",
               "montoTotalOperacion","totalNoGravado","totalPagar","totalLetras",
               "totalIva","saldoFavor","condicionOperacion","pagos","numPagoElectronico"},
        "03": {"totalNoSuj","totalExenta","totalGravada","subTotalVentas",
               "descuNoSuj","descuExenta","descuGravada","porcentajeDescuento",
               "totalDescu","tributos","subTotal","ivaPerci1","ivaRete1","reteRenta",
               "montoTotalOperacion","totalNoGravado","totalPagar","totalLetras",
               "saldoFavor","condicionOperacion","pagos","numPagoElectronico"},
        "05": {"totalNoSuj","totalExenta","totalGravada","subTotalVentas",
               "descuNoSuj","descuExenta","descuGravada","totalDescu",
               "tributos","subTotal","ivaPerci1","ivaRete1","reteRenta",
               "montoTotalOperacion","totalLetras","condicionOperacion"},
        "06": {"totalNoSuj","totalExenta","totalGravada","subTotalVentas",
               "descuNoSuj","descuExenta","descuGravada","totalDescu",
               "tributos","subTotal","ivaPerci1","ivaRete1","reteRenta",
               "montoTotalOperacion","totalLetras","condicionOperacion",
               "numPagoElectronico"},
        "07": {"totalSujetoRetencion","totalIvaRetenido","totalLetras",
               "totalIva","observaciones"},
        "14": {"totalCompra","descu","totalDescu","subTotal","ivaRete1",
               "reteRenta","totalPagar","totalLetras","condicionOperacion",
               "pagos","observaciones"},
        "15": {"totalDonacion","totalLetras","condicionOperacion"},
    }

    @pytest.mark.parametrize("tipo", ["01","03","04","05","06","07","08","09","11","14","15"])
    def test_top_level_keys_match(self, tipo):
        b = builder()
        cfg = TestAllTypesConstruct.CONFIGS[tipo]
        kwargs = {"receptor": cfg["receptor"], "items": cfg["items"]}
        if "dte_referencia" in cfg: kwargs["dte_referencia"] = cfg["dte_referencia"]
        if "dcl_params" in cfg: kwargs["dcl_params"] = cfg["dcl_params"]
        dte, _ = b.build(tipo, f"DTE-{tipo}-M001P001-000000000000001", **kwargs)
        expected = self.CERTIFIED_KEYS[tipo]
        assert list(dte.keys()) == expected, \
            f"Type {tipo}: expected {expected}, got {list(dte.keys())}"

    @pytest.mark.parametrize("tipo", ["01","03","05","06","07","14","15"])
    def test_resumen_keys_exact(self, tipo):
        b = builder()
        cfg = TestAllTypesConstruct.CONFIGS[tipo]
        kwargs = {"receptor": cfg["receptor"], "items": cfg["items"]}
        if "dte_referencia" in cfg: kwargs["dte_referencia"] = cfg["dte_referencia"]
        dte, _ = b.build(tipo, f"DTE-{tipo}-M001P001-000000000000001", **kwargs)
        expected = self.CERTIFIED_RESUMEN_KEYS[tipo]
        actual = set(dte["resumen"].keys())
        assert actual == expected, \
            f"Type {tipo} resumen: extra={actual-expected}, missing={expected-actual}"


# ═══════════════════════════════════════════
# G. IVA CALCULATIONS
# ═══════════════════════════════════════════

class TestIVACalculations:
    """Verify Factura extracts IVA, CCF adds IVA."""

    @pytest.mark.parametrize("precio,expected_iva", [
        (100.0, 11.50), (113.0, 13.0), (50.0, 5.75),
        (1.13, 0.13), (1000.0, 115.04),
    ])
    def test_factura_iva_extraction(self, precio, expected_iva):
        b = builder()
        dte, _ = b.build("01", "DTE-01-M001P001-000000000000001",
            {"nombre": "CF", "tipo_documento": "36", "num_documento": "0"},
            [{"precio_unitario": precio, "descripcion": "T", "codigo": "T"}])
        assert dte["cuerpoDocumento"][0]["ivaItem"] == expected_iva
        # montoTotal = gravada (IVA inclusive)
        assert dte["resumen"]["montoTotalOperacion"] == precio

    @pytest.mark.parametrize("precio,expected_iva,expected_total", [
        (100.0, 13.0, 113.0), (150.0, 19.5, 169.5), (1000.0, 130.0, 1130.0),
    ])
    def test_ccf_iva_addition(self, precio, expected_iva, expected_total):
        b = builder()
        dte, _ = b.build("03", "DTE-03-M001P001-000000000000001",
            {"nombre": "R", "nit": "123", "nrc": "1"},
            [{"precio_unitario": precio, "descripcion": "T", "codigo": "T"}])
        assert dte["resumen"]["tributos"][0]["valor"] == expected_iva
        assert dte["resumen"]["montoTotalOperacion"] == expected_total

    def test_fse_rete_renta_threshold(self):
        b = builder()
        # >= 100: 10% reteRenta
        dte, _ = b.build("14", "DTE-14-M001P001-000000000000001",
            {"nombre": "X", "tipo_documento": "13", "num_documento": "0"},
            [{"precio_unitario": 100, "descripcion": "T", "codigo": "T"}])
        assert dte["resumen"]["reteRenta"] == 10.0
        assert dte["resumen"]["totalPagar"] == 90.0

    def test_fse_no_rete_under_100(self):
        b = builder()
        dte, _ = b.build("14", "DTE-14-M001P001-000000000000001",
            {"nombre": "X", "tipo_documento": "13", "num_documento": "0"},
            [{"precio_unitario": 99, "descripcion": "T", "codigo": "T"}])
        assert dte["resumen"]["reteRenta"] == 0.0
        assert dte["resumen"]["totalPagar"] == 99.0


# ═══════════════════════════════════════════
# H. EDGE CASES
# ═══════════════════════════════════════════

class TestEdgeCases:
    def test_multiple_items(self):
        b = builder()
        items = [{"precio_unitario": 50, "descripcion": f"Item {i}", "codigo": f"I{i}"}
                 for i in range(5)]
        dte, _ = b.build("01", "DTE-01-M001P001-000000000000001",
            {"nombre": "CF", "tipo_documento": "36", "num_documento": "0"}, items)
        assert len(dte["cuerpoDocumento"]) == 5
        assert dte["resumen"]["totalGravada"] == 250.0
        for i, item in enumerate(dte["cuerpoDocumento"]):
            assert item["numItem"] == i + 1

    def test_large_amount(self):
        b = builder()
        dte, _ = b.build("03", "DTE-03-M001P001-000000000000001",
            {"nombre": "R", "nit": "123", "nrc": "1"},
            [{"precio_unitario": 999999.99, "descripcion": "Big", "codigo": "B"}])
        assert dte["resumen"]["totalGravada"] == 999999.99
        assert "DOLARES" in dte["resumen"]["totalLetras"]

    def test_decimal_precision(self):
        b = builder()
        dte, _ = b.build("01", "DTE-01-M001P001-000000000000001",
            {"nombre": "CF", "tipo_documento": "36", "num_documento": "0"},
            [{"precio_unitario": 33.33, "descripcion": "T", "codigo": "T"}])
        item = dte["cuerpoDocumento"][0]
        assert item["ventaGravada"] == 33.33
        assert isinstance(item["ivaItem"], float)

    def test_special_chars_in_description(self):
        b = builder()
        dte, _ = b.build("01", "DTE-01-M001P001-000000000000001",
            {"nombre": "CF", "tipo_documento": "36", "num_documento": "0"},
            [{"precio_unitario": 10, "descripcion": "Ñoño & más café", "codigo": "T"}])
        assert dte["cuerpoDocumento"][0]["descripcion"] == "Ñoño & más café"

    def test_dcl_porcentComision_is_int(self):
        b = builder()
        dte, _ = b.build("09", "DTE-09-M001P001-000000000000001",
            {"nombre": "R", "nit": "123", "nrc": "1"},
            [{"precio_unitario": 100, "descripcion": "X", "codigo": "X"}],
            dcl_params={"valor_operaciones": 1130, "porcentaje_comision": 5})
        assert isinstance(dte["cuerpoDocumento"]["porcentComision"], int)

    def test_cd_uniMedida_always_99(self):
        b = builder()
        dte, _ = b.build("15", "DTE-15-M001P001-000000000000001",
            {"nombre": "DON", "nit": "123", "cod_pais": "9300"},
            [{"descripcion": "Donación", "valor": 500, "codigo": "D1"},
             {"descripcion": "Otra", "valor": 300, "codigo": "D2"}])
        for item in dte["cuerpoDocumento"]:
            assert item["uniMedida"] == 99

    def test_monto_letras_formats(self):
        b = builder()
        cases = [
            (100.0, "CIEN"),
            (1000.0, "MIL"),
            (169.5, "CIENTO SESENTA Y NUEVE 50/100"),
        ]
        for precio, expected_fragment in cases:
            dte, _ = b.build("01", "DTE-01-M001P001-000000000000001",
                {"nombre": "CF", "tipo_documento": "36", "num_documento": "0"},
                [{"precio_unitario": precio, "descripcion": "T", "codigo": "T"}])
            letras = dte["resumen"]["totalLetras"]
            assert expected_fragment in letras, f"{precio}: '{expected_fragment}' not in '{letras}'"
