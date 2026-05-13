"""Wompi service unit tests — all network calls mocked."""
import os
from unittest.mock import AsyncMock, patch

import pytest

from app.services import wompi_service
from app.services.wompi_service import (
    WompiError,
    _reset_token_cache,
    create_payment_link,
    verify_payment,
)


@pytest.fixture(autouse=True)
def _wompi_env(monkeypatch):
    monkeypatch.setenv("WOMPI_APP_ID", "test-app-id")
    monkeypatch.setenv("WOMPI_API_SECRET", "test-secret")
    _reset_token_cache()
    yield
    _reset_token_cache()


def _make_response(status: int, json_body=None, text: str = ""):
    """Build a stand-in httpx.Response object for AsyncClient mocks."""
    m = AsyncMock()
    m.status_code = status
    m.json = lambda: json_body if json_body is not None else {}
    m.text = text or (str(json_body) if json_body is not None else "")
    return m


class _MockHttpClient:
    """Async context manager whose .post/.get yield queued responses."""

    def __init__(self, responses_by_path: dict):
        self.responses_by_path = responses_by_path
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def _pick(self, url: str):
        for key, resp in self.responses_by_path.items():
            if key in url:
                return resp
        raise AssertionError(f"No mock for {url}")

    async def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self._pick(url)

    async def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self._pick(url)


# ─── Auth flow ───────────────────────────────────────────────────

class TestTokenExchange:
    @pytest.mark.asyncio
    async def test_missing_credentials_raises(self, monkeypatch):
        monkeypatch.delenv("WOMPI_APP_ID", raising=False)
        monkeypatch.delenv("WOMPI_API_SECRET", raising=False)
        _reset_token_cache()
        with pytest.raises(WompiError) as exc:
            await wompi_service._get_access_token()
        assert exc.value.status == 500
        assert "WOMPI_APP_ID" in exc.value.message

    @pytest.mark.asyncio
    async def test_token_cached_until_expiry(self):
        token_resp = _make_response(200, {"access_token": "abc", "expires_in": 3600})
        mock_client = _MockHttpClient({"connect/token": token_resp})
        with patch("httpx.AsyncClient", return_value=mock_client):
            t1 = await wompi_service._get_access_token()
            t2 = await wompi_service._get_access_token()
        assert t1 == t2 == "abc"
        # Auth endpoint called exactly once across the two requests.
        assert sum(1 for c in mock_client.calls if "connect/token" in c[1]) == 1

    @pytest.mark.asyncio
    async def test_non_200_raises(self):
        token_resp = _make_response(401, {}, text="invalid_client")
        mock_client = _MockHttpClient({"connect/token": token_resp})
        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(WompiError) as exc:
                await wompi_service._get_access_token()
        assert exc.value.status == 502

    @pytest.mark.asyncio
    async def test_missing_access_token_field_raises(self):
        token_resp = _make_response(200, {"expires_in": 3600})
        mock_client = _MockHttpClient({"connect/token": token_resp})
        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(WompiError):
                await wompi_service._get_access_token()


# ─── create_payment_link ─────────────────────────────────────────

class TestCreatePaymentLink:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        token_resp = _make_response(200, {"access_token": "tok", "expires_in": 3600})
        link_resp = _make_response(201, {
            "idEnlace": "WLINK-123",
            "urlCompleta": "https://checkout.wompi.sv/pay/WLINK-123",
            "monto": 10.0,
        })
        mock_client = _MockHttpClient({
            "connect/token": token_resp,
            "EnlacesPago": link_resp,
        })
        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await create_payment_link(
                amount_usd=10.0, credits=100,
                org_id="org-uuid-1234abcd",
                org_name="ACME", customer_email="a@b.com",
                return_url="https://example.com/back",
            )
        assert result["payment_url"] == "https://checkout.wompi.sv/pay/WLINK-123"
        assert result["payment_id"] == "WLINK-123"
        assert result["reference"].startswith("FSV-100cr-")

    @pytest.mark.asyncio
    async def test_rejects_zero_amount(self):
        with pytest.raises(WompiError):
            await create_payment_link(
                amount_usd=0, credits=10,
                org_id="o", org_name="x", customer_email="a@b.com",
                return_url="https://e/",
            )

    @pytest.mark.asyncio
    async def test_response_without_url_or_id_raises(self):
        token_resp = _make_response(200, {"access_token": "tok", "expires_in": 3600})
        bad_resp = _make_response(200, {"monto": 10.0})  # missing url + id
        mock_client = _MockHttpClient({
            "connect/token": token_resp,
            "EnlacesPago": bad_resp,
        })
        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(WompiError):
                await create_payment_link(
                    amount_usd=10.0, credits=100,
                    org_id="o", org_name="x", customer_email="a@b.com",
                    return_url="https://e/",
                )


# ─── verify_payment ──────────────────────────────────────────────

class TestVerifyPayment:
    @pytest.mark.asyncio
    async def test_paid(self):
        token_resp = _make_response(200, {"access_token": "tok", "expires_in": 3600})
        verify_resp = _make_response(200, {
            "monto": 10.0,
            "infoProducto": {
                "identificadorOrg": "org-abc",
                "cantidadCreditos": "100",
            },
            "transaccionCompra": {
                "resultado": {"estado": "AprobAdA"},
            },
        })
        mock_client = _MockHttpClient({
            "connect/token": token_resp,
            "EnlacesPago/": verify_resp,
        })
        with patch("httpx.AsyncClient", return_value=mock_client):
            info = await verify_payment("WLINK-123")
        assert info["is_paid"] is True
        assert info["credits"] == 100
        assert info["org_id"] == "org-abc"
        assert info["amount"] == 10.0

    @pytest.mark.asyncio
    async def test_pending(self):
        token_resp = _make_response(200, {"access_token": "tok", "expires_in": 3600})
        verify_resp = _make_response(200, {
            "monto": 10.0,
            "infoProducto": {"cantidadCreditos": "100"},
            # No transaccionCompra → not paid yet
        })
        mock_client = _MockHttpClient({
            "connect/token": token_resp,
            "EnlacesPago/": verify_resp,
        })
        with patch("httpx.AsyncClient", return_value=mock_client):
            info = await verify_payment("WLINK-123")
        assert info["is_paid"] is False

    @pytest.mark.asyncio
    async def test_404_raises(self):
        token_resp = _make_response(200, {"access_token": "tok", "expires_in": 3600})
        not_found = _make_response(404, {}, text="not found")
        mock_client = _MockHttpClient({
            "connect/token": token_resp,
            "EnlacesPago/": not_found,
        })
        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(WompiError) as exc:
                await verify_payment("WLINK-MISSING")
        assert exc.value.status == 404

    @pytest.mark.asyncio
    async def test_invalid_credits_field_returns_zero(self):
        token_resp = _make_response(200, {"access_token": "tok", "expires_in": 3600})
        verify_resp = _make_response(200, {
            "infoProducto": {"cantidadCreditos": "not-a-number"},
            "transaccionCompra": {"resultado": {"estado": "aprobada"}},
        })
        mock_client = _MockHttpClient({
            "connect/token": token_resp,
            "EnlacesPago/": verify_resp,
        })
        with patch("httpx.AsyncClient", return_value=mock_client):
            info = await verify_payment("WLINK-X")
        assert info["credits"] == 0
