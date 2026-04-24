"""
FACTURA-SV: Test Suite — Credit Pricing
========================================
Validates flat $0.10/DTE pricing. Each credit includes:
  - DTE emission + MH transmission + digital signature + PDF
  - Email delivery
  - WhatsApp delivery
Credits never expire. No monthly fees. No volume discount.

Run: python -m pytest tests/test_pricing.py -v
"""
import pytest
from app.routers.credits_router import calculate_price, PRICE_PER_DTE


class TestFlatPricing:
    """Flat $0.10/DTE pricing."""

    def test_price_per_dte_is_ten_cents(self):
        assert PRICE_PER_DTE == 0.10

    def test_small_quantity(self):
        unit, total, disc = calculate_price(100)
        assert unit == 0.10
        assert total == 10.00
        assert disc == 0.0

    def test_medium_quantity(self):
        unit, total, disc = calculate_price(500)
        assert unit == 0.10
        assert total == 50.00
        assert disc == 0.0

    def test_large_quantity(self):
        unit, total, disc = calculate_price(1000)
        assert unit == 0.10
        assert total == 100.00
        assert disc == 0.0

    def test_higher_volume(self):
        unit, total, _ = calculate_price(5000)
        assert unit == 0.10
        assert total == 500.00

    def test_enterprise_volume(self):
        unit, total, _ = calculate_price(10000)
        assert unit == 0.10
        assert total == 1000.00

    def test_unit_price_is_always_ten_cents(self):
        """Price is always $0.10 regardless of quantity."""
        for qty in [1, 50, 100, 999, 5000, 50000]:
            unit, _, _ = calculate_price(qty)
            assert unit == 0.10, f"qty={qty}: expected 0.10, got {unit}"

    def test_discount_always_zero(self):
        """No volume discount in flat pricing."""
        for qty in [100, 1000, 10000, 100000]:
            _, _, disc = calculate_price(qty)
            assert disc == 0.0

    def test_total_scales_linearly(self):
        """total == qty * 0.10."""
        test_cases = {
            100: 10.00,
            500: 50.00,
            1000: 100.00,
            2000: 200.00,
            5000: 500.00,
            10000: 1000.00,
        }
        for qty, expected in test_cases.items():
            _, total, _ = calculate_price(qty)
            assert total == expected, f"qty={qty}: expected {expected}, got {total}"

    def test_zero_quantity(self):
        unit, total, disc = calculate_price(0)
        assert unit == 0.10
        assert total == 0.0
        assert disc == 0.0

    def test_negative_quantity(self):
        unit, total, _ = calculate_price(-5)
        assert unit == 0.10
        assert total == 0.0

    def test_return_types(self):
        result = calculate_price(100)
        assert isinstance(result, tuple)
        assert len(result) == 3
        unit, total, disc = result
        assert isinstance(unit, float)
        assert isinstance(total, float)
        assert isinstance(disc, float)
