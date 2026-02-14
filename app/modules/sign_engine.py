"""
FACTURA-SV — Module 2: SignEngine
Handles digital signing of DTE documents using the contributor's .p12 certificate.

Flow:
1. User uploads .p12 file + password (per session, not persisted)
2. We extract the private key and certificate from the .p12
3. For each DTE, we sign the JSON body as a JWT (RS256)
4. The signed JWT is what gets transmitted to the MH

MH Signing Specification:
- Format: JSON Web Token (JWT)
- Algorithm: RS256 (RSA + SHA-256) using the private key from .p12
- The DTE JSON is the JWT payload
- The certificate must be issued by the DGII
- Private key is processed ONLY in memory, never written to disk
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional
from io import BytesIO

import jwt  # PyJWT
from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.primitives.serialization import BestAvailableEncryption
from cryptography.x509 import Certificate
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey

logger = logging.getLogger(__name__)


class SignEngineError(Exception):
    """Raised when signing operations fail."""
    def __init__(self, message: str, code: str = "SIGN_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class CertificateSession:
    """
    Holds the extracted private key and certificate info for the duration
    of a user session. NOT persisted to disk or database.
    """

    def __init__(
        self,
        private_key_pem: bytes,
        certificate: Certificate,
        cert_chain: list[Certificate],
    ):
        self._private_key_pem = private_key_pem
        self._certificate = certificate
        self._cert_chain = cert_chain
        self._created_at = datetime.now(timezone.utc)

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
        """Try to extract NIT from certificate subject (CN or serialNumber)."""
        for attr in self._certificate.subject:
            oid_name = attr.oid.dotted_string
            value = attr.value
            # Common patterns for NIT in SV certificates
            if "2.5.4.5" in oid_name or "serialNumber" in str(attr.oid):
                return value
            if "2.5.4.3" in oid_name:  # CN - may contain NIT
                if any(c.isdigit() for c in value) and "-" in value:
                    return value
        return None

    def to_dict(self) -> dict:
        return {
            "subject": self.subject,
            "issuer": self.issuer,
            "serial_number": self.serial_number,
            "valid_from": self.valid_from.isoformat(),
            "valid_to": self.valid_to.isoformat(),
            "is_valid": self.is_valid_now,
            "nit_in_cert": self.get_nit_from_subject(),
            "loaded_at": self._created_at.isoformat(),
        }

    def destroy(self):
        """Explicitly clear sensitive data from memory."""
        self._private_key_pem = b"\x00" * len(self._private_key_pem)
        self._private_key_pem = None
        logger.info("Certificate session destroyed — private key cleared from memory.")


class SignEngine:
    """
    Manages .p12 certificate loading and DTE signing.

    Usage:
        engine = SignEngine()
        session = engine.load_certificate(p12_bytes, password)
        signed_jwt = engine.sign_dte(session, dte_json)
    """

    def load_certificate(self, p12_data: bytes, password: str) -> CertificateSession:
        """
        Load a .p12/.pfx certificate file and extract private key + cert.

        Args:
            p12_data: Raw bytes of the .p12 file
            password: Password for the .p12 file

        Returns:
            CertificateSession with extracted key and certificate info

        Raises:
            SignEngineError: If loading fails
        """
        try:
            pwd_bytes = password.encode("utf-8") if password else None

            # Extract private key, certificate, and optional chain
            private_key, certificate, cert_chain = pkcs12.load_key_and_certificates(
                p12_data, pwd_bytes
            )

            if private_key is None:
                raise SignEngineError(
                    "El archivo .p12 no contiene una clave privada.",
                    code="CERT_NO_PRIVATE_KEY",
                )

            if certificate is None:
                raise SignEngineError(
                    "El archivo .p12 no contiene un certificado.",
                    code="CERT_NO_CERTIFICATE",
                )

            # Serialize private key to PEM (in memory only)
            private_key_pem = private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )

            session = CertificateSession(
                private_key_pem=private_key_pem,
                certificate=certificate,
                cert_chain=list(cert_chain) if cert_chain else [],
            )

            # Validate certificate is not expired
            if not session.is_valid_now:
                raise SignEngineError(
                    f"El certificado expiró el {session.valid_to.strftime('%Y-%m-%d')}. "
                    f"Renuévelo en factura.gob.sv.",
                    code="CERT_EXPIRED",
                )

            logger.info(
                f"Certificate loaded: subject={session.subject}, "
                f"valid_to={session.valid_to.isoformat()}, "
                f"nit={session.get_nit_from_subject()}"
            )

            return session

        except SignEngineError:
            raise

        except ValueError as e:
            if "password" in str(e).lower() or "mac" in str(e).lower():
                raise SignEngineError(
                    "Contraseña incorrecta para el archivo .p12.",
                    code="CERT_WRONG_PASSWORD",
                ) from e
            raise SignEngineError(
                f"Error al leer el archivo .p12: {str(e)}",
                code="CERT_INVALID_FORMAT",
            ) from e

        except Exception as e:
            logger.exception(f"Error loading .p12 certificate: {e}")
            raise SignEngineError(
                f"Error inesperado al cargar el certificado: {str(e)}",
                code="CERT_LOAD_ERROR",
            ) from e

    def sign_dte(self, session: CertificateSession, dte_json: dict) -> str:
        """
        Sign a DTE JSON document as a JWT using the session's private key.

        The MH expects the DTE wrapped in a JWT signed with RS256.
        The complete DTE JSON becomes the JWT payload.

        Args:
            session: Active CertificateSession with loaded private key
            dte_json: Complete DTE document as a dictionary

        Returns:
            JWT string (header.payload.signature)

        Raises:
            SignEngineError: If signing fails
        """
        if session.private_key_pem is None:
            raise SignEngineError(
                "La sesión del certificado ha sido destruida. Cargue el certificado nuevamente.",
                code="CERT_SESSION_DESTROYED",
            )

        if not session.is_valid_now:
            raise SignEngineError(
                "El certificado ha expirado. Cargue un certificado vigente.",
                code="CERT_EXPIRED",
            )

        try:
            # The MH expects the entire DTE as the JWT payload
            # Algorithm: RS256 (RSA + SHA-256)
            # Decode PEM bytes to str for broader PyJWT version compatibility
            pem_key = (
                session.private_key_pem.decode("utf-8")
                if isinstance(session.private_key_pem, bytes)
                else session.private_key_pem
            )
            signed_token = jwt.encode(
                payload=dte_json,
                key=pem_key,
                algorithm="RS256",
            )

            logger.info(
                f"DTE signed successfully. "
                f"Type={dte_json.get('identificacion', {}).get('tipoDte', '?')}, "
                f"CodigoGen={dte_json.get('identificacion', {}).get('codigoGeneracion', '?')[:8]}..."
            )

            return signed_token

        except Exception as e:
            logger.exception(f"Error signing DTE: {e}")
            raise SignEngineError(
                f"Error al firmar el DTE: {str(e)}",
                code="SIGN_FAILED",
            ) from e


# Singleton instance
sign_engine = SignEngine()
