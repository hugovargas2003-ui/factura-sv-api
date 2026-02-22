#!/usr/bin/env python3
"""
FACTURA-SV — Integration Test Script
=====================================
Ejecuta pruebas de integración contra la API desplegada.

Uso:
  # Solo health check (sin credenciales)
  python tests/test_integration.py https://tu-dominio.railway.app

  # Flujo completo (requiere credenciales MH de prueba)
  python tests/test_integration.py https://tu-dominio.railway.app \
    --nit 0614-123456-789-0 \
    --password MiClavePrueba \
    --p12 /ruta/a/certificado.p12 \
    --p12-password clave_del_p12

Requisitos:
  pip install httpx
"""

import sys
import pytest
pytestmark = pytest.mark.skip(reason="Standalone script, not a pytest test")
import argparse
import time

try:
    import httpx
except ImportError:
    print("ERROR: pip install httpx")
    sys.exit(1)


class Colors:
    OK = "\033[92m"
    FAIL = "\033[91m"
    WARN = "\033[93m"
    BOLD = "\033[1m"
    END = "\033[0m"


passed = 0
failed = 0


def test(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        print(f"  {Colors.OK}✓{Colors.END} {name}")
        passed += 1
    else:
        print(f"  {Colors.FAIL}✗{Colors.END} {name}: {detail}")
        failed += 1
    return condition


def run_tests(base_url: str, nit: str = None, password: str = None,
              p12_path: str = None, p12_password: str = None):
    global passed, failed

    client = httpx.Client(base_url=base_url, timeout=30.0)

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}1. HEALTH CHECK{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    r = client.get("/health")
    test("GET /health → 200", r.status_code == 200)

    if r.status_code == 200:
        data = r.json()
        test("status = ok", data.get("status") == "ok")
        test("version presente", "version" in data)
        test("environment presente", "environment" in data)
        test("mh_auth_url presente", "mh_auth_url" in data)
        env = data.get("environment", "")
        print(f"    → Entorno: {Colors.WARN}{env}{Colors.END}")
        print(f"    → Versión: {data.get('version')}")
        print(f"    → MH URL: {data.get('mh_auth_url')}")

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}2. SWAGGER / DOCS{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    r = client.get("/docs")
    test("GET /docs → 200", r.status_code == 200)
    test("Swagger HTML", "swagger" in r.text.lower() or "openapi" in r.text.lower())

    r = client.get("/openapi.json")
    test("GET /openapi.json → 200", r.status_code == 200)

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}3. UTILIDADES{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    r = client.get("/utils/generate-uuid")
    test("GET /utils/generate-uuid → 200", r.status_code == 200)
    if r.status_code == 200:
        uuid_val = r.json().get("codigoGeneracion", "")
        test("UUID formato válido (36 chars)", len(uuid_val) == 36)

    r = client.get("/utils/generate-numero-control?tipo_dte=03")
    test("GET /utils/generate-numero-control → 200", r.status_code == 200)
    if r.status_code == 200:
        nc = r.json().get("numeroControl", "")
        test("NumControl = 32 chars", len(nc) == 32)
        test("NumControl empieza con DTE-03-", nc.startswith("DTE-03-"))

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}4. PROTECCIÓN DE ENDPOINTS{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    # Sin session_id debería dar 401
    r = client.get("/session")
    test("GET /session sin auth → 401", r.status_code == 401)

    r = client.post("/transmit", json={"dte_json": {}, "tipo_dte": "03"})
    test("POST /transmit sin auth → 401", r.status_code == 401)

    r = client.post("/query", json={"nit_emisor": "x", "tipo_dte": "03", "codigo_generacion": "x"})
    test("POST /query sin auth → 401", r.status_code == 401)

    # ═══════════════════════════════════════════════════════════
    # Si no hay credenciales, terminar aquí
    # ═══════════════════════════════════════════════════════════

    if not all([nit, password]):
        print(f"\n{Colors.WARN}⚠ Sin credenciales MH — omitiendo flujo completo.{Colors.END}")
        print(f"  Use --nit, --password, --p12, --p12-password para el flujo completo.\n")
        return

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}5. AUTENTICACIÓN CON MH{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    r = client.post("/auth", json={"nit": nit, "password": password})
    test("POST /auth → 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:200]}")

    session_id = r.headers.get("X-Session-Id")
    test("X-Session-Id en header", session_id is not None)

    if not session_id:
        print(f"  {Colors.FAIL}ABORTANDO: Sin session_id, no se puede continuar.{Colors.END}")
        return

    print(f"    → Session ID: {session_id[:12]}...")

    auth_data = r.json()
    test("status = authenticated", auth_data.get("status") == "authenticated")

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}6. SESIÓN{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    r = client.get("/session", headers={"X-Session-Id": session_id})
    test("GET /session → 200", r.status_code == 200)
    if r.status_code == 200:
        s = r.json()
        test("Sesión tiene NIT", "nit" in s)
        test("Certificado no cargado aún", s.get("certificate_loaded") is False or "cert" in str(s))

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}7. CARGA DE CERTIFICADO{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    if not p12_path:
        print(f"  {Colors.WARN}⚠ Sin certificado .p12 — omitiendo firma y transmisión.{Colors.END}")
        # Cleanup: cerrar sesión
        r = client.delete("/session", headers={"X-Session-Id": session_id})
        test("DELETE /session → 200", r.status_code == 200)
        return

    with open(p12_path, "rb") as f:
        files = {"certificate": (p12_path.split("/")[-1], f, "application/x-pkcs12")}
        data = {"password": p12_password or "", "session_id": session_id}
        r = client.post("/certificate", files=files, data=data)

    test("POST /certificate → 200", r.status_code == 200, f"HTTP {r.status_code}: {r.text[:300]}")

    if r.status_code == 200:
        cert_info = r.json()
        test("Certificado válido", cert_info.get("is_valid") is True)
        test("NIT en certificado", cert_info.get("nit_in_cert") is not None)
        print(f"    → Subject: {cert_info.get('subject')}")
        print(f"    → Válido hasta: {cert_info.get('valid_to')}")
    else:
        print(f"  {Colors.FAIL}ABORTANDO: Certificado no cargado.{Colors.END}")
        client.delete("/session", headers={"X-Session-Id": session_id})
        return

    # ═══════════════════════════════════════════════════════════
    print(f"\n{Colors.BOLD}8. CERRAR SESIÓN{Colors.END}")
    # ═══════════════════════════════════════════════════════════

    r = client.delete("/session", headers={"X-Session-Id": session_id})
    test("DELETE /session → 200", r.status_code == 200)

    # Verificar que la sesión ya no existe
    r = client.get("/session", headers={"X-Session-Id": session_id})
    test("Sesión destruida (401)", r.status_code == 401)

    client.close()


def main():
    parser = argparse.ArgumentParser(description="FACTURA-SV Integration Tests")
    parser.add_argument("base_url", help="URL base de la API (ej: https://factura-sv.up.railway.app)")
    parser.add_argument("--nit", help="NIT para autenticación MH")
    parser.add_argument("--password", help="Contraseña de Oficina Virtual")
    parser.add_argument("--p12", help="Ruta al archivo .p12")
    parser.add_argument("--p12-password", help="Contraseña del .p12")
    args = parser.parse_args()

    url = args.base_url.rstrip("/")
    print(f"\n{'=' * 60}")
    print(f"FACTURA-SV — Integration Tests")
    print(f"Target: {url}")
    print(f"{'=' * 60}")

    start = time.time()
    run_tests(url, args.nit, args.password, args.p12, args.p12_password)
    elapsed = time.time() - start

    print(f"\n{'=' * 60}")
    total = passed + failed
    if failed:
        print(f"{Colors.FAIL}RESULTADO: {passed}/{total} pasaron, {failed} fallaron ({elapsed:.1f}s){Colors.END}")
        sys.exit(1)
    else:
        print(f"{Colors.OK}RESULTADO: {passed}/{total} pasaron ({elapsed:.1f}s){Colors.END}")
        print(f"{Colors.OK}✅ ALL INTEGRATION TESTS PASSED{Colors.END}")


if __name__ == "__main__":
    main()
