"""
FACTURA-SV: Test Suite — Encryption Service
=============================================
Validates per-org key derivation and encrypt/decrypt cycle.

Run: python -m pytest tests/test_encryption.py -v
"""
import pytest
import os
from app.services.encryption_service import EncryptionService


MASTER_KEY = EncryptionService.generate_master_key()


class TestEncryptionBasic:
    """Core encrypt/decrypt cycle tests."""

    def setup_method(self):
        self.svc = EncryptionService(master_key=MASTER_KEY)

    def test_encrypt_decrypt_bytes(self):
        data = b"Hello World"
        encrypted = self.svc.encrypt(data, "org-1")
        decrypted = self.svc.decrypt(encrypted, "org-1")
        assert decrypted == data

    def test_encrypt_decrypt_string(self):
        text = "Mi contraseña secreta MH"
        encrypted = self.svc.encrypt_string(text, "org-1")
        decrypted = self.svc.decrypt_string(encrypted, "org-1")
        assert decrypted == text

    def test_encrypted_differs_from_plaintext(self):
        data = b"sensitive data"
        encrypted = self.svc.encrypt(data, "org-1")
        assert encrypted != data

    def test_empty_string(self):
        encrypted = self.svc.encrypt_string("", "org-1")
        assert self.svc.decrypt_string(encrypted, "org-1") == ""

    def test_unicode_text(self):
        text = "Contraseña con ñ, ü, é, 日本語"
        encrypted = self.svc.encrypt_string(text, "org-1")
        assert self.svc.decrypt_string(encrypted, "org-1") == text

    def test_large_data(self):
        """Simulate encrypting a .p12 certificate (~4KB)."""
        data = os.urandom(4096)
        encrypted = self.svc.encrypt(data, "org-1")
        assert self.svc.decrypt(encrypted, "org-1") == data


class TestPerOrgIsolation:
    """Verify org-level key isolation."""

    def setup_method(self):
        self.svc = EncryptionService(master_key=MASTER_KEY)

    def test_different_orgs_different_keys(self):
        key1 = self.svc._derive_key("org-1")
        key2 = self.svc._derive_key("org-2")
        assert key1 != key2

    def test_cross_org_decrypt_fails(self):
        """Data encrypted for org-1 cannot be decrypted by org-2."""
        data = b"org-1 secret"
        encrypted = self.svc.encrypt(data, "org-1")
        with pytest.raises(Exception):
            self.svc.decrypt(encrypted, "org-2")

    def test_same_org_same_key(self):
        """Same org always derives the same key."""
        key1 = self.svc._derive_key("org-1")
        key2 = self.svc._derive_key("org-1")
        assert key1 == key2


class TestKeyGeneration:
    """Master key generation tests."""

    def test_generate_key_is_valid_fernet(self):
        key = EncryptionService.generate_master_key()
        assert len(key) == 44  # Fernet key is 44 chars base64
        svc = EncryptionService(master_key=key)
        data = b"test"
        assert svc.decrypt(svc.encrypt(data, "org-x"), "org-x") == data

    def test_different_master_keys_different_derived(self):
        svc1 = EncryptionService(master_key=EncryptionService.generate_master_key())
        svc2 = EncryptionService(master_key=EncryptionService.generate_master_key())
        assert svc1._derive_key("org-1") != svc2._derive_key("org-1")


class TestV1ToV2Migration:
    """Transparent migration from SHA256 (v1) to HKDF (v2)."""

    def setup_method(self):
        self.svc = EncryptionService(master_key=MASTER_KEY)

    def test_v2_differs_from_v1(self):
        """HKDF and SHA256 produce different keys."""
        v1 = self.svc._derive_key_v1("org-1")
        v2 = self.svc._derive_key_v2("org-1")
        assert v1 != v2

    def test_v1_encrypted_data_decrypts_via_fallback(self):
        """Data encrypted with v1 key can still be decrypted (fallback)."""
        data = b"legacy password"
        # Encrypt with v1 key directly
        from cryptography.fernet import Fernet
        v1_fernet = Fernet(self.svc._derive_key_v1("org-1"))
        encrypted_v1 = v1_fernet.encrypt(data)
        # Decrypt via service (should fallback to v1)
        decrypted = self.svc.decrypt(encrypted_v1, "org-1")
        assert decrypted == data

    def test_re_encrypt_migrates_v1_to_v2(self):
        """re_encrypt() converts v1-encrypted data to v2."""
        data = b"migrate me"
        from cryptography.fernet import Fernet
        v1_fernet = Fernet(self.svc._derive_key_v1("org-1"))
        encrypted_v1 = v1_fernet.encrypt(data)
        # Re-encrypt
        encrypted_v2 = self.svc.re_encrypt(encrypted_v1, "org-1")
        assert encrypted_v2 is not None
        # Now v2 key decrypts directly
        v2_fernet = Fernet(self.svc._derive_key_v2("org-1"))
        assert v2_fernet.decrypt(encrypted_v2) == data

    def test_re_encrypt_returns_none_if_already_v2(self):
        """re_encrypt() returns None if data is already v2."""
        data = b"already v2"
        encrypted_v2 = self.svc.encrypt(data, "org-1")
        result = self.svc.re_encrypt(encrypted_v2, "org-1")
        assert result is None

    def test_new_encryptions_use_v2(self):
        """New encrypt() calls use HKDF v2 key."""
        data = b"new data"
        encrypted = self.svc.encrypt(data, "org-1")
        # Should decrypt with v2 key directly (no fallback needed)
        from cryptography.fernet import Fernet
        v2_fernet = Fernet(self.svc._derive_key_v2("org-1"))
        assert v2_fernet.decrypt(encrypted) == data
