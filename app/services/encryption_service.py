"""
FACTURA-SV: Servicio de Encriptación
=====================================
Encripta/desencripta certificados digitales y contraseñas MH
usando Fernet (symmetric encryption) con key derivada por org.

v2: HKDF key derivation (stronger than SHA256 concatenation).
Transparent migration: decrypt attempts v2 first, falls back to v1.

Uso:
    svc = EncryptionService()
    encrypted = svc.encrypt(data_bytes, org_id)
    decrypted = svc.decrypt(encrypted_bytes, org_id)
"""
import os
import hashlib
import base64
import logging
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

logger = logging.getLogger("encryption")


class EncryptionService:
    """Encriptación multi-tenant con key derivada por organización (HKDF v2)."""

    KEY_VERSION = 2  # Current version: HKDF

    def __init__(self, master_key: str | None = None):
        self._master_key = (master_key or os.environ["ENCRYPTION_MASTER_KEY"]).encode()

    # ── Key Derivation ──

    def _derive_key(self, org_id: str) -> bytes:
        """Deriva Fernet key usando HKDF (v2)."""
        return self._derive_key_v2(org_id)

    def _derive_key_v2(self, org_id: str) -> bytes:
        """HKDF-SHA256 derivation (v2 — current)."""
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=org_id.encode(),
            info=b"factura-sv-org-key-v2",
        )
        derived = hkdf.derive(self._master_key)
        return base64.urlsafe_b64encode(derived)

    def _derive_key_v1(self, org_id: str) -> bytes:
        """SHA256 concatenation (v1 — legacy, for migration)."""
        raw = hashlib.sha256(self._master_key + org_id.encode()).digest()
        return base64.urlsafe_b64encode(raw)

    def _fernet(self, org_id: str) -> Fernet:
        return Fernet(self._derive_key(org_id))

    def _fernet_v1(self, org_id: str) -> Fernet:
        return Fernet(self._derive_key_v1(org_id))

    # ── Public API ──

    def encrypt(self, data: bytes, org_id: str) -> bytes:
        """Encripta bytes con key HKDF derivada de org_id."""
        return self._fernet(org_id).encrypt(data)

    def decrypt(self, token: bytes, org_id: str) -> bytes:
        """Desencripta bytes. Intenta v2 (HKDF), fallback a v1 (SHA256 legacy)."""
        try:
            return self._fernet(org_id).decrypt(token)
        except InvalidToken:
            # Transparent migration: try legacy v1 key
            try:
                data = self._fernet_v1(org_id).decrypt(token)
                logger.info(f"Decrypted with legacy v1 key for org {org_id[:8]}...")
                return data
            except InvalidToken:
                raise  # Neither key works — data is corrupted or wrong org

    def encrypt_string(self, text: str, org_id: str) -> bytes:
        """Encripta un string (contraseña MH)."""
        return self.encrypt(text.encode("utf-8"), org_id)

    def decrypt_string(self, token: bytes, org_id: str) -> str:
        """Desencripta a string."""
        return self.decrypt(token, org_id).decode("utf-8")

    def re_encrypt(self, token: bytes, org_id: str) -> bytes | None:
        """Re-encrypt data from v1 to v2 if needed. Returns None if already v2."""
        try:
            self._fernet(org_id).decrypt(token)
            return None  # Already v2
        except InvalidToken:
            pass
        # Decrypt with v1, re-encrypt with v2
        data = self._fernet_v1(org_id).decrypt(token)
        return self._fernet(org_id).encrypt(data)

    @staticmethod
    def generate_master_key() -> str:
        """Genera una master key nueva para ENCRYPTION_MASTER_KEY."""
        return Fernet.generate_key().decode()
