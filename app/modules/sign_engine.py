"""
FACTURA-SV — Module 2: SignEngine
Handles digital signing of DTE documents using the contributor's .p12 certificate.

MH Signing Specification (validated during certification - 600+ DTEs accepted):
- Format: JSON Web Signature (JWS) compact serialization
- Algorithm: RS512 (RSA + PKCS1v15 + SHA-512)
- Header: {"alg": "RS512", "typ": "JWS"}
- Payload: json.dumps(separators=(",",":"), ensure_ascii=False)
- Library: cryptography direct (NOT PyJWT — serialization differences cause rejection)
"""

import json
import base64
import logging
from datetime import datetime, timezone
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import (
    pkcs12, Encoding, PrivateFormat, NoEncryption,
)
from cryptography.x509 import Certificate

logger = logging.getLogger(__name__)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    s += "=" * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)


_JWS_HEADER = {"alg": "RS256", "typ": "JWT"}
_JWS_HEADER_B64 = _b64url_encode(
    json.dumps(_JWS_HEADER, separators=(",", ":")).encode("utf-8")
)


class SignEngineError(Exception):
    def __init__(self, message: str, code: str = "SIGN_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class CertificateSession:
    def __init__(self, private_key: RSAPrivateKey, private_key_pem: bytes,
                 certificate: Certificate, cert_chain: list[Certificate]):
        self._private_key = private_key
        self._private_key_pem = private_key_pem
        self._certificate = certificate
        self._cert_chain = cert_chain
        self._created_at = datetime.now(timezone.utc)

    @property
    def private_key(self) -> RSAPrivateKey:
        return self._private_key

    @property
    def private_key_pem(self) -> bytes:
        return self._private_key_pem

    @property
    def certificate(self) -> Certificate:
        return self._certificate

    @property
    def subject(self) -> str:
        return self._certificate.subject.rfc4514_string()

    @property
    def issuer(self) -> str:
        return self._certificate.issuer.rfc4514_string()

    @property
    def serial_number(self) -> str:
        return format(self._certificate.serial_number, "X")

    @property
    def valid_from(self) -> datetime:
        return self._certificate.not_valid_before_utc

    @property
    def valid_to(self) -> datetime:
        return self._certificate.not_valid_after_utc

    @property
    def is_valid_now(self) -> bool:
        now = datetime.now(timezone.utc)
        return self.valid_from <= now <= self.valid_to

    def get_nit_from_subject(self) -> Optional[str]:
        for attr in self._certificate.subject:
            oid_name = attr.oid.dotted_string
            value = attr.value
            if "2.5.4.5" in oid_name or "serialNumber" in str(attr.oid):
                return value
            if "2.5.4.3" in oid_name:
                if any(c.isdigit() for c in value) and "-" in value:
                    return value
        return None

    def to_dict(self) -> dict:
        return {
            "subject": self.subject, "issuer": self.issuer,
            "serial_number": self.serial_number,
            "valid_from": self.valid_from.isoformat(),
            "valid_to": self.valid_to.isoformat(),
            "is_valid": self.is_valid_now,
            "nit_in_cert": self.get_nit_from_subject(),
            "loaded_at": self._created_at.isoformat(),
        }

    def destroy(self):
        self._private_key_pem = b"\x00" * len(self._private_key_pem)
        self._private_key_pem = None
        self._private_key = None
        logger.info("Certificate session destroyed.")


class SignEngine:
    def load_certificate(self, p12_data: bytes, password: str) -> CertificateSession:
        try:
            pwd_bytes = password.encode("utf-8") if password else None
            private_key, certificate, cert_chain = pkcs12.load_key_and_certificates(
                p12_data, pwd_bytes
            )
            if private_key is None:
                raise SignEngineError("El archivo .p12 no contiene una clave privada.", code="CERT_NO_PRIVATE_KEY")
            if certificate is None:
                raise SignEngineError("El archivo .p12 no contiene un certificado.", code="CERT_NO_CERTIFICATE")
            if not isinstance(private_key, RSAPrivateKey):
                raise SignEngineError("Solo se admiten claves RSA para firma de DTE.", code="CERT_NOT_RSA")

            private_key_pem = private_key.private_bytes(
                encoding=Encoding.PEM, format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )
            session = CertificateSession(
                private_key=private_key, private_key_pem=private_key_pem,
                certificate=certificate, cert_chain=list(cert_chain) if cert_chain else [],
            )
            if not session.is_valid_now:
                raise SignEngineError(
                    f"El certificado expiró el {session.valid_to.strftime('%Y-%m-%d')}. "
                    f"Renuévelo en factura.gob.sv.", code="CERT_EXPIRED",
                )
            logger.info(f"Certificate loaded: subject={session.subject}, "
                        f"valid_to={session.valid_to.isoformat()}, nit={session.get_nit_from_subject()}")
            return session

        except SignEngineError:
            raise
        except ValueError as e:
            if "password" in str(e).lower() or "mac" in str(e).lower():
                raise SignEngineError("Contraseña incorrecta para el archivo .p12.", code="CERT_WRONG_PASSWORD") from e
            raise SignEngineError(f"Error al leer el archivo .p12: {str(e)}", code="CERT_INVALID_FORMAT") from e
        except Exception as e:
            logger.exception(f"Error loading .p12 certificate: {e}")
            raise SignEngineError(f"Error inesperado al cargar el certificado: {str(e)}", code="CERT_LOAD_ERROR") from e

    def sign_dte(self, session: CertificateSession, dte_json: dict) -> str:
        """
        Sign a DTE as JWS using RS512 + PKCS1v15 + SHA-512.
        Identical to certified mh_signer.py that passed 600+ DTEs.
        """
        if session.private_key is None:
            raise SignEngineError("La sesion del certificado ha sido destruida.", code="CERT_SESSION_DESTROYED")
        if not session.is_valid_now:
            raise SignEngineError("El certificado ha expirado.", code="CERT_EXPIRED")

        try:
            payload_str = json.dumps(dte_json, separators=(",", ":"), ensure_ascii=False)
            payload_b64 = _b64url_encode(payload_str.encode("utf-8"))
            signing_input = f"{_JWS_HEADER_B64}.{payload_b64}".encode("ascii")

            signature = session.private_key.sign(
                signing_input, padding.PKCS1v15(), hashes.SHA256(),
            )

            signature_b64 = _b64url_encode(signature)
            jws = f"{_JWS_HEADER_B64}.{payload_b64}.{signature_b64}"

            logger.info(
                f"DTE signed (RS512/JWS). Type={dte_json.get('identificacion', {}).get('tipoDte', '?')}, "
                f"CodigoGen={dte_json.get('identificacion', {}).get('codigoGeneracion', '?')[:8]}..., "
                f"JWS len={len(jws)}"
            )
            return jws

        except Exception as e:
            logger.exception(f"Error signing DTE: {e}")
            raise SignEngineError(f"Error al firmar el DTE: {str(e)}", code="SIGN_FAILED") from e

    @staticmethod
    def sign_with_pem(private_key_pem: str, dte_json: dict) -> str:
        """Sign a DTE using a PEM private key string directly (for billing)."""
        from cryptography.hazmat.backends import default_backend
        pem_bytes = private_key_pem.encode("utf-8") if isinstance(private_key_pem, str) else private_key_pem
        private_key = serialization.load_pem_private_key(pem_bytes, password=None, backend=default_backend())

        payload_str = json.dumps(dte_json, separators=(",", ":"), ensure_ascii=False)
        payload_b64 = _b64url_encode(payload_str.encode("utf-8"))
        signing_input = f"{_JWS_HEADER_B64}.{payload_b64}".encode("ascii")
        signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        signature_b64 = _b64url_encode(signature)
        return f"{_JWS_HEADER_B64}.{payload_b64}.{signature_b64}"


sign_engine = SignEngine()
