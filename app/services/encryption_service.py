"""
FACTURA-SV: Servicio de Encriptación
=====================================
Encripta/desencripta certificados digitales y contraseñas MH
usando Fernet (symmetric encryption) con key derivada por org.

Uso:
    svc = EncryptionService()
    encrypted = svc.encrypt(data_bytes, org_id)
    decrypted = svc.decrypt(encrypted_bytes, org_id)
"""
import os
import hashlib
import base64
from cryptography.fernet import Fernet


class EncryptionService:
    """Encriptación multi-tenant con key derivada por organización."""

    def __init__(self, master_key: str | None = None):
        self._master_key = (master_key or os.environ["ENCRYPTION_MASTER_KEY"]).encode()

    def _derive_key(self, org_id: str) -> bytes:
        """Deriva una Fernet key única por organización."""
        raw = hashlib.sha256(self._master_key + org_id.encode()).digest()
        return base64.urlsafe_b64encode(raw)

    def _fernet(self, org_id: str) -> Fernet:
        return Fernet(self._derive_key(org_id))

    # ── Public API ──

    def encrypt(self, data: bytes, org_id: str) -> bytes:
        """Encripta bytes con key derivada de org_id."""
        return self._fernet(org_id).encrypt(data)

    def decrypt(self, token: bytes, org_id: str) -> bytes:
        """Desencripta bytes con key derivada de org_id."""
        return self._fernet(org_id).decrypt(token)

    def encrypt_string(self, text: str, org_id: str) -> bytes:
        """Encripta un string (contraseña MH)."""
        return self.encrypt(text.encode("utf-8"), org_id)

    def decrypt_string(self, token: bytes, org_id: str) -> str:
        """Desencripta a string."""
        return self.decrypt(token, org_id).decode("utf-8")

    @staticmethod
    def generate_master_key() -> str:
        """Genera una master key nueva para ENCRYPTION_MASTER_KEY."""
        return Fernet.generate_key().decode()
