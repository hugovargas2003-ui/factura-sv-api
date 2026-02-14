"""
FACTURA-SV — Unit Tests
Tests for critical modules: helpers, sign_engine, config, session logic.

Run: pytest tests/ -v
"""

import pytest
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# ─────────────────────────────────────────────────────────────
# DTE HELPERS
# ─────────────────────────────────────────────────────────────

from app.utils.dte_helpers import (
    generate_codigo_generacion,
    generate_numero_control,
    current_sv_datetime,
    validate_nit,
    validate_nrc,
)


class TestGenerateCodigoGeneracion:
    def test_returns_uuid_format(self):
        code = generate_codigo_generacion()
        # UUID v4 format: 8-4-4-4-12, uppercase
        parts = code.split("-")
        assert len(parts) == 5
        assert len(code) == 36
        assert code == code.upper()

    def test_returns_unique_values(self):
        codes = {generate_codigo_generacion() for _ in range(100)}
        assert len(codes) == 100

    def test_is_valid_uuid(self):
        code = generate_codigo_generacion()
        parsed = uuid.UUID(code, version=4)
        assert str(parsed).upper() == code


class TestGenerateNumeroControl:
    def test_default_format(self):
        nc = generate_numero_control("03")
        assert nc == "DTE-03-M001-P001-000000000000001"
        assert len(nc) == 32

    def test_custom_params(self):
        nc = generate_numero_control("01", "M002", "P003", 42)
        assert nc == "DTE-01-M002-P003-000000000000042"

    def test_large_correlativo(self):
        nc = generate_numero_control("11", correlativo=999999999999999)
        assert nc.endswith("999999999999999")
        assert len(nc) == 32

    def test_all_dte_types(self):
        for tipo in ["01", "03", "04", "05", "06", "07", "08", "09", "11", "14", "15"]:
            nc = generate_numero_control(tipo)
            assert nc.startswith(f"DTE-{tipo}-")
            assert len(nc) == 32


class TestCurrentSVDatetime:
    def test_returns_tuple(self):
        fecha, hora = current_sv_datetime()
        assert len(fecha) == 10  # YYYY-MM-DD
        assert len(hora) == 8   # HH:MM:SS
        assert "-" in fecha
        assert ":" in hora


class TestValidateNIT:
    def test_valid_nit(self):
        assert validate_nit("0614-123456-789-0") is True

    def test_invalid_format_missing_parts(self):
        assert validate_nit("0614-123456-789") is False

    def test_invalid_format_wrong_lengths(self):
        assert validate_nit("06-123456-789-0") is False
        assert validate_nit("0614-12345-789-0") is False
        assert validate_nit("0614-123456-78-0") is False
        assert validate_nit("0614-123456-789-01") is False

    def test_invalid_non_numeric(self):
        assert validate_nit("061A-123456-789-0") is False

    def test_empty_string(self):
        assert validate_nit("") is False


class TestValidateNRC:
    def test_valid_nrc(self):
        assert validate_nrc("123456-7") is True

    def test_invalid_format(self):
        assert validate_nrc("123456") is False
        assert validate_nrc("12345-78") is False
        assert validate_nrc("") is False
        assert validate_nrc("ABCDEF-7") is False


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

from app.core.config import get_mh_url, MHEnvironment, MH_URLS


class TestConfig:
    def test_all_environments_have_all_services(self):
        services = ["auth", "recepcion_dte", "consulta_dte", "anulacion_dte", "contingencia"]
        for env in MHEnvironment:
            for service in services:
                url = MH_URLS[env][service]
                assert url.startswith("https://")

    def test_test_urls_contain_test(self):
        for service, url in MH_URLS[MHEnvironment.TEST].items():
            assert "test" in url.lower(), f"Test URL for {service} should contain 'test': {url}"

    def test_production_urls_do_not_contain_test(self):
        for service, url in MH_URLS[MHEnvironment.PRODUCTION].items():
            assert "test" not in url.lower(), f"Prod URL for {service} should not contain 'test': {url}"

    def test_get_mh_url_invalid_service_raises(self):
        with pytest.raises(ValueError, match="Unknown MH service"):
            get_mh_url("nonexistent_service")


# ─────────────────────────────────────────────────────────────
# SIGN ENGINE (certificate loading — uses self-signed test cert)
# ─────────────────────────────────────────────────────────────

from app.modules.sign_engine import SignEngine, SignEngineError, CertificateSession


def _create_test_p12(common_name: str = "0614-123456-789-0", password: str = "testpass") -> bytes:
    """Create a self-signed test .p12 certificate for testing."""
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12, BestAvailableEncryption

    # Generate key
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Build certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test DGII"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "SV"),
        x509.NameAttribute(NameOID.SERIAL_NUMBER, common_name),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime(2025, 1, 1, tzinfo=timezone.utc))
        .not_valid_after(datetime(2027, 12, 31, tzinfo=timezone.utc))
        .sign(key, hashes.SHA256())
    )

    return pkcs12.serialize_key_and_certificates(
        name=b"test",
        key=key,
        cert=cert,
        cas=None,
        encryption_algorithm=BestAvailableEncryption(password.encode()),
    )


class TestSignEngine:
    def setup_method(self):
        self.engine = SignEngine()
        self.test_password = "testpass123"
        self.test_p12 = _create_test_p12(password=self.test_password)

    def test_load_valid_certificate(self):
        session = self.engine.load_certificate(self.test_p12, self.test_password)
        assert isinstance(session, CertificateSession)
        assert session.is_valid_now
        assert session.private_key_pem is not None
        assert "Test DGII" in session.issuer

    def test_load_wrong_password_raises(self):
        with pytest.raises(SignEngineError) as exc_info:
            self.engine.load_certificate(self.test_p12, "wrong_password")
        assert exc_info.value.code == "CERT_WRONG_PASSWORD"

    def test_load_invalid_data_raises(self):
        with pytest.raises(SignEngineError):
            self.engine.load_certificate(b"not a p12 file", "pass")

    def test_load_empty_data_raises(self):
        with pytest.raises(SignEngineError):
            self.engine.load_certificate(b"", "pass")

    def test_sign_dte_produces_jwt(self):
        session = self.engine.load_certificate(self.test_p12, self.test_password)
        dte = {
            "identificacion": {"tipoDte": "03", "codigoGeneracion": "TEST-UUID"},
            "emisor": {"nit": "0614-123456-789-0"},
        }
        signed = self.engine.sign_dte(session, dte)
        assert isinstance(signed, str)
        # JWT has 3 parts separated by dots
        parts = signed.split(".")
        assert len(parts) == 3

    def test_sign_after_destroy_raises(self):
        session = self.engine.load_certificate(self.test_p12, self.test_password)
        session.destroy()
        with pytest.raises(SignEngineError) as exc_info:
            self.engine.sign_dte(session, {"test": True})
        assert exc_info.value.code == "CERT_SESSION_DESTROYED"

    def test_certificate_nit_extraction(self):
        session = self.engine.load_certificate(self.test_p12, self.test_password)
        nit = session.get_nit_from_subject()
        assert nit == "0614-123456-789-0"

    def test_certificate_to_dict(self):
        session = self.engine.load_certificate(self.test_p12, self.test_password)
        info = session.to_dict()
        assert "subject" in info
        assert "issuer" in info
        assert "is_valid" in info
        assert info["is_valid"] is True

    def test_destroy_clears_key(self):
        session = self.engine.load_certificate(self.test_p12, self.test_password)
        assert session.private_key_pem is not None
        session.destroy()
        assert session.private_key_pem is None


# ─────────────────────────────────────────────────────────────
# AUTH BRIDGE (mocked HTTP)
# ─────────────────────────────────────────────────────────────

from app.modules.auth_bridge import AuthBridge, AuthBridgeError, TokenInfo


class TestTokenInfo:
    def test_token_not_expired_initially(self):
        token = TokenInfo("fake_token", "0614-123456-789-0", MHEnvironment.TEST)
        assert not token.is_expired
        assert token.bearer == "Bearer fake_token"

    def test_token_expired_after_time(self):
        token = TokenInfo("fake_token", "0614-123456-789-0", MHEnvironment.PRODUCTION)
        # Force expiry
        token.expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        assert token.is_expired

    def test_to_dict(self):
        token = TokenInfo("abcdef1234567890", "0614-123456-789-0", MHEnvironment.TEST)
        d = token.to_dict()
        assert d["nit"] == "0614-123456-789-0"
        assert d["is_expired"] is False
        assert "token_preview" in d


class TestAuthBridge:
    """AuthBridge only exposes authenticate() which requires HTTP.
    We test that the class can be instantiated without errors."""

    def test_instantiation(self):
        bridge = AuthBridge()
        assert bridge is not None


# ─────────────────────────────────────────────────────────────
# TRANSMIT SERVICE (response parsing)
# ─────────────────────────────────────────────────────────────

from app.modules.transmit_service import TransmitService, TransmitError, DTE_SCHEMA_VERSIONS
import httpx


class TestTransmitServiceParsing:
    def setup_method(self):
        self.service = TransmitService()

    def test_parse_successful_response(self):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "estado": "PROCESADO",
            "selloRecibido": "ABCDEF1234567890",
            "codigoGeneracion": "TEST-UUID-123",
            "fhProcesamiento": "13/02/2026 10:30:00",
            "clasificaMsg": "01",
            "codigoMsg": "001",
            "descripcionMsg": "EXITO",
            "observaciones": [],
        }

        result = self.service._parse_response(mock_response, "TEST-UUID-123")
        assert result.status == "PROCESADO"
        assert result.sello_recepcion == "ABCDEF1234567890"
        assert result.observaciones == []

    def test_parse_rejected_response(self):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "estado": "RECHAZADO",
            "observaciones": ["NIT inválido", "Código actividad no existe"],
            "descripcionMsg": "Error de validación",
        }

        result = self.service._parse_response(mock_response, "TEST-UUID")
        assert result.status == "RECHAZADO"
        assert len(result.observaciones) == 2

    def test_parse_401_raises(self):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 401
        mock_response.json.return_value = {"message": "Token expired"}

        with pytest.raises(TransmitError) as exc_info:
            self.service._parse_response(mock_response, "TEST")
        assert exc_info.value.status_code == 401

    def test_parse_non_json_raises(self):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 502
        mock_response.json.side_effect = Exception("Not JSON")
        mock_response.text = "<html>Bad Gateway</html>"

        with pytest.raises(TransmitError) as exc_info:
            self.service._parse_response(mock_response, "TEST")
        assert "no-JSON" in exc_info.value.message

    def test_all_dte_types_have_schema_version(self):
        expected_types = ["01", "03", "04", "05", "06", "07", "08", "09", "11", "14", "15"]
        for t in expected_types:
            assert t in DTE_SCHEMA_VERSIONS, f"Missing schema version for DTE type {t}"
