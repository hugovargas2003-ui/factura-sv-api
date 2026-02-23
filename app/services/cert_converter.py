"""
FACTURA-SV: Convertidor CertificadoMH XML → .p12
==================================================
Extrae la llave privada RSA del XML del MH y genera
un archivo .p12 listo para firmar DTEs.

Diferenciador clave: los clientes solo necesitan subir
el archivo .crt que MH les da, sin proceso manual.
"""
import base64
import datetime
import secrets
import xml.etree.ElementTree as ET
from cryptography.hazmat.primitives.serialization import load_der_private_key
from cryptography.hazmat.primitives.serialization.pkcs12 import serialize_key_and_certificates
from cryptography.hazmat.primitives.serialization import BestAvailableEncryption
from cryptography.hazmat.primitives import hashes
from cryptography import x509
from cryptography.x509.oid import NameOID


def convert_mh_cert_to_p12(cert_content: bytes) -> tuple[bytes, str]:
    """
    Convierte CertificadoMH XML a .p12.
    Returns: (p12_bytes, password)
    """
    # Parse XML
    text = cert_content.decode("utf-8", errors="replace")
    root = ET.fromstring(text)

    # Extract NIT
    nit_el = root.find("nit")
    nit = nit_el.text if nit_el is not None else "unknown"

    # Extract private key (PKCS#8 DER, base64-encoded)
    priv_key_el = root.find(".//privateKey/encodied")
    if priv_key_el is None or not priv_key_el.text:
        raise ValueError("No se encontró la llave privada en el CertificadoMH")

    priv_b64 = priv_key_el.text.replace("\n", "").replace("\r", "").strip()
    priv_der = base64.b64decode(priv_b64)
    private_key = load_der_private_key(priv_der, password=None)

    # Extract subject info for self-signed cert
    subject_el = root.find(".//subject")
    org_name = "Contribuyente"
    if subject_el is not None:
        org_el = subject_el.find("organizationName")
        if org_el is not None and org_el.text:
            org_name = org_el.text

    # Create self-signed certificate wrapper
    subject = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "SV"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, org_name[:64]),
        x509.NameAttribute(NameOID.COMMON_NAME, nit),
    ])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2024, 1, 1))
        .not_valid_after(datetime.datetime(2035, 12, 31))
        .sign(private_key, hashes.SHA256())
    )

    # Generate random password for .p12
    p12_password = secrets.token_hex(8)  # 16 chars

    # Create .p12
    p12_bytes = serialize_key_and_certificates(
        name=f"MH-{nit}".encode(),
        key=private_key,
        cert=cert,
        cas=None,
        encryption_algorithm=BestAvailableEncryption(p12_password.encode()),
    )

    return p12_bytes, p12_password
