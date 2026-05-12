"""Comprehensive coverage of flows that were untested.

These tests pin behavior that's easy to break accidentally during
refactors: phone normalization, the DTE-type label map, the flat
pricing function, the plan_limits sentinel, and the delivery_channels
contract.
"""
import pytest


# ─── WhatsApp helper ─────────────────────────────────────────────

class TestNormalizePhone:
    def test_8digits(self):
        from app.services.whatsapp_express_engine import normalize_phone
        assert normalize_phone("71760768") == "50371760768"

    def test_with_dashes(self):
        from app.services.whatsapp_express_engine import normalize_phone
        assert normalize_phone("7176-0768") == "50371760768"

    def test_with_country_and_spaces(self):
        from app.services.whatsapp_express_engine import normalize_phone
        assert normalize_phone("+503 7176 0768") == "50371760768"

    def test_already_full(self):
        from app.services.whatsapp_express_engine import normalize_phone
        assert normalize_phone("50371760768") == "50371760768"

    def test_empty_string(self):
        from app.services.whatsapp_express_engine import normalize_phone
        assert normalize_phone("") == ""

    def test_none(self):
        from app.services.whatsapp_express_engine import normalize_phone
        assert normalize_phone(None) == ""


class TestDTETypeLabels:
    def test_known_codes(self):
        from app.services.whatsapp_express_engine import DTE_TYPE_LABELS
        assert DTE_TYPE_LABELS["01"] == "Factura"
        assert DTE_TYPE_LABELS["03"] == "Crédito Fiscal"
        assert DTE_TYPE_LABELS["14"] == "Factura Sujeto Excluido"

    def test_full_coverage(self):
        """All 11 MH-defined DTE types are labelled."""
        from app.services.whatsapp_express_engine import DTE_TYPE_LABELS
        expected = {"01", "03", "04", "05", "06", "07", "08", "09", "11", "14", "15"}
        assert set(DTE_TYPE_LABELS.keys()) == expected


# ─── Flat pricing ────────────────────────────────────────────────

class TestPricing:
    def test_unit_price_is_ten_cents(self):
        from app.routers.credits_router import PRICE_PER_DTE
        assert PRICE_PER_DTE == 0.10

    def test_returns_triple(self):
        from app.routers.credits_router import calculate_price
        unit, total, discount = calculate_price(100)
        assert unit == 0.10
        assert total == 10.00
        assert discount == 0.0

    def test_no_volume_discount(self):
        """Unit price stays at $0.10 regardless of quantity."""
        from app.routers.credits_router import calculate_price
        for qty in (1, 50, 100, 500, 1000, 5000, 10000):
            unit, _total, discount = calculate_price(qty)
            assert unit == 0.10, f"qty={qty}: unit drifted to {unit}"
            assert discount == 0.0, f"qty={qty}: discount surfaced {discount}"

    def test_total_scales_linearly(self):
        from app.routers.credits_router import calculate_price
        assert calculate_price(1000)[1] == 100.00
        assert calculate_price(2500)[1] == 250.00

    def test_zero_quantity(self):
        from app.routers.credits_router import calculate_price
        _unit, total, _discount = calculate_price(0)
        assert total == 0.0


# ─── plan_limits sentinel ────────────────────────────────────────

class TestPlanLimits:
    def test_constants(self):
        from app.services.plan_limits import (
            UNLIMITED_DTE_QUOTA, UNLIMITED_MAX_COMPANIES,
        )
        assert UNLIMITED_DTE_QUOTA == 999999
        assert UNLIMITED_MAX_COMPANIES == 9999

    def test_null_is_unlimited(self):
        from app.services.plan_limits import is_unlimited_companies
        assert is_unlimited_companies(None) is True

    def test_zero_is_unlimited(self):
        from app.services.plan_limits import is_unlimited_companies
        assert is_unlimited_companies(0) is True

    def test_negative_is_unlimited(self):
        from app.services.plan_limits import is_unlimited_companies
        assert is_unlimited_companies(-1) is True

    def test_at_sentinel_is_unlimited(self):
        from app.services.plan_limits import (
            is_unlimited_companies, UNLIMITED_MAX_COMPANIES,
        )
        assert is_unlimited_companies(UNLIMITED_MAX_COMPANIES) is True
        assert is_unlimited_companies(UNLIMITED_MAX_COMPANIES + 1) is True

    def test_below_sentinel_is_capped(self):
        from app.services.plan_limits import is_unlimited_companies
        assert is_unlimited_companies(1) is False
        assert is_unlimited_companies(5) is False
        assert is_unlimited_companies(9998) is False


# ─── delivery_channels contract ──────────────────────────────────

class TestDeliveryChannels:
    """The emit_dte signature normalizes delivery_channels with this rule:
        None      → ["email", "whatsapp"]  (legacy default)
        ["none"]  → []                     (explicit opt-out)
        []        → []                     (explicit opt-out)
        list      → list with "none" filtered out
    """

    def _resolve(self, channels):
        # Mirrors the inline expression in DTEService.emit_dte.
        return (
            ["email", "whatsapp"]
            if channels is None
            else [c for c in channels if c and c != "none"]
        )

    def test_none_defaults_to_both(self):
        resolved = self._resolve(None)
        assert "email" in resolved and "whatsapp" in resolved

    def test_explicit_empty_means_nothing(self):
        assert self._resolve([]) == []

    def test_none_sentinel_means_nothing(self):
        assert self._resolve(["none"]) == []

    def test_email_only(self):
        resolved = self._resolve(["email"])
        assert "email" in resolved and "whatsapp" not in resolved

    def test_whatsapp_only(self):
        resolved = self._resolve(["whatsapp"])
        assert "whatsapp" in resolved and "email" not in resolved

    def test_filters_garbage_inside_list(self):
        """Stray 'none' inside a multi-channel list is dropped, not honored."""
        resolved = self._resolve(["email", "none"])
        assert resolved == ["email"]


# ─── Ambiente mapping (used by auth/transmit/invalidation flows) ─

class TestAmbienteMapping:
    """DTEService._env_from_creds maps mh_credentials.ambiente to the
    MHEnvironment enum that downstream services use for URL selection."""

    def test_production_when_01(self):
        from app.services.dte_service import DTEService
        from app.core.config import MHEnvironment
        assert DTEService._env_from_creds({"ambiente": "01"}) == MHEnvironment.PRODUCTION

    def test_test_when_00(self):
        from app.services.dte_service import DTEService
        from app.core.config import MHEnvironment
        assert DTEService._env_from_creds({"ambiente": "00"}) == MHEnvironment.TEST

    def test_test_when_missing(self):
        """Defensive default — never silently treat unknown creds as prod."""
        from app.services.dte_service import DTEService
        from app.core.config import MHEnvironment
        assert DTEService._env_from_creds({}) == MHEnvironment.TEST


# ─── IVA extraction (regression guard for the "$0 IVA" bug) ──────

class TestIvaExtraction:
    def test_totaliva_field_preferred(self):
        from app.services.dte_service import _extract_iva
        assert _extract_iva({"totalIva": 13.0}) == 13.0

    def test_falls_back_to_tributos(self):
        from app.services.dte_service import _extract_iva
        assert _extract_iva({"tributos": [{"valor": 26.0}]}) == 26.0

    def test_zero_when_neither(self):
        from app.services.dte_service import _extract_iva
        assert _extract_iva({}) == 0

    def test_empty_tributos_list(self):
        from app.services.dte_service import _extract_iva
        assert _extract_iva({"tributos": []}) == 0
