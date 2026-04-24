"""
FACTURA-SV: Test Suite — Credit Pricing Algorithm
==================================================
Validates the logarithmic pricing formula:
  precio_por_dte = max(P_min, P_base - K * ln(cantidad))

Default params: P_base=0.13, P_min=0.03, K=0.013

Run: python -m pytest tests/test_pricing.py -v
"""
import pytest
from app.routers.credits_router import calculate_price

# Default pricing parameters
P_BASE = 0.13
P_MIN = 0.03
K = 0.013


class TestPricingFormula:
    """Core pricing algorithm tests."""

    def test_small_quantity_100(self):
        unit, total, disc = calculate_price(100, P_BASE, P_MIN, K)
        assert unit == 0.0701
        assert total == 7.01

    def test_medium_quantity_500(self):
        unit, total, _ = calculate_price(500, P_BASE, P_MIN, K)
        assert unit > P_MIN
        assert total == round(unit * 500, 2)

    def test_large_quantity_1000(self):
        unit, total, _ = calculate_price(1000, P_BASE, P_MIN, K)
        assert unit > P_MIN
        assert total == round(unit * 1000, 2)

    def test_floor_kicks_in(self):
        """At high quantities, price hits the floor."""
        unit, total, _ = calculate_price(5000, P_BASE, P_MIN, K)
        assert unit == P_MIN
        assert total == round(P_MIN * 5000, 2)

    def test_very_large_quantity_stays_at_floor(self):
        unit, total, _ = calculate_price(50000, P_BASE, P_MIN, K)
        assert unit == P_MIN
        assert total == round(P_MIN * 50000, 2)

    def test_unit_price_decreases_with_quantity(self):
        """Logarithmic: more credits → lower unit price."""
        prices = []
        for qty in [10, 50, 100, 500, 1000, 2000]:
            unit, _, _ = calculate_price(qty, P_BASE, P_MIN, K)
            prices.append(unit)
        for i in range(1, len(prices)):
            assert prices[i] <= prices[i - 1], \
                f"Price should decrease: {prices[i]} > {prices[i-1]}"

    def test_total_increases_with_quantity(self):
        """Total cost always increases even though unit price drops."""
        totals = []
        for qty in [10, 100, 500, 1000, 5000]:
            _, total, _ = calculate_price(qty, P_BASE, P_MIN, K)
            totals.append(total)
        for i in range(1, len(totals)):
            assert totals[i] > totals[i - 1]

    def test_zero_quantity_returns_default(self):
        unit, total, disc = calculate_price(0, P_BASE, P_MIN, K)
        assert unit == 0.07
        assert total == 0.0

    def test_negative_quantity_returns_default(self):
        unit, total, disc = calculate_price(-5, P_BASE, P_MIN, K)
        assert unit == 0.07

    def test_discount_percentage(self):
        """Discount is calculated vs base rate of $0.0701."""
        unit, _, disc = calculate_price(100, P_BASE, P_MIN, K)
        if unit < 0.0701:
            expected_disc = round((1 - unit / 0.0701) * 100, 1)
            assert disc == expected_disc
        else:
            assert disc == 0.0

    def test_floor_discount_is_maximum(self):
        """At floor price, discount should be highest."""
        _, _, disc_small = calculate_price(100, P_BASE, P_MIN, K)
        _, _, disc_large = calculate_price(10000, P_BASE, P_MIN, K)
        assert disc_large >= disc_small


class TestPricingEdgeCases:
    """Boundary and edge case tests."""

    def test_minimum_recharge_10(self):
        unit, total, _ = calculate_price(10, P_BASE, P_MIN, K)
        assert total > 0
        assert unit > 0

    def test_custom_params(self):
        """Pricing works with different base/min/k."""
        unit, total, _ = calculate_price(100, 0.20, 0.05, 0.02)
        assert unit >= 0.05
        assert total == round(unit * 100, 2)

    def test_return_types(self):
        result = calculate_price(100, P_BASE, P_MIN, K)
        assert isinstance(result, tuple)
        assert len(result) == 3
        unit, total, disc = result
        assert isinstance(unit, float)
        assert isinstance(total, float)
        assert isinstance(disc, float)
