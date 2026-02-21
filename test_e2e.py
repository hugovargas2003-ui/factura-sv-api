"""
FACTURA-SV: E2E Test Script
Genera JWT para usuario existente y prueba endpoints.
"""
import requests
from supabase import create_client

SUPABASE_URL = "https://fcqevmujdjfbdcyjdmzg.supabase.co"
SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjcWV2bXVqZGpmYmRjeWpkbXpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDk4ODY0NywiZXhwIjoyMDg2NTY0NjQ3fQ.hzQ98V5QfhbcHV3K0aLFD4xLD4ClIYs0LuB04pq_fAc"
ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjcWV2bXVqZGpmYmRjeWpkbXpnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA5ODg2NDcsImV4cCI6MjA4NjU2NDY0N30.Autrt4WaORiezFIGjRyAg-vrZ798CIEet2Tk1zkCHNQ"
API_URL = "https://factura-sv-api-production.up.railway.app"
TEST_EMAIL = "test-e2e@factura-sv.com"
TEST_PASS = "TestE2E-2026!"

sb = create_client(SUPABASE_URL, SERVICE_KEY)

# Step 0: Crear usuario de prueba (o reusar si existe)
print("=" * 60)
print("STEP 0: Crear/obtener usuario de prueba")
print("=" * 60)

try:
    user_resp = sb.auth.admin.create_user({
        "email": TEST_EMAIL,
        "password": TEST_PASS,
        "email_confirm": True,
    })
    user_id = user_resp.user.id
    print(f"  ✅ Usuario creado: {user_id}")
except Exception as e:
    if "already been registered" in str(e) or "already exists" in str(e):
        # Buscar usuario existente
        users = sb.auth.admin.list_users()
        user_id = None
        for u in users:
            if hasattr(u, 'email') and u.email == TEST_EMAIL:
                user_id = u.id
                break
            elif isinstance(u, list):
                for uu in u:
                    if hasattr(uu, 'email') and uu.email == TEST_EMAIL:
                        user_id = uu.id
                        break
        if user_id:
            print(f"  ℹ️  Usuario ya existe: {user_id}")
        else:
            print(f"  ❌ No se encontró usuario: {e}")
            exit(1)
    else:
        print(f"  ❌ Error: {e}")
        exit(1)

# Asegurar que existe en tabla users con org
print("\n  Verificando tabla users...")
existing = sb.table("users").select("*").eq("id", user_id).execute()
if not existing.data:
    # Obtener org existente
    orgs = sb.table("organizations").select("id").limit(1).execute()
    if orgs.data:
        org_id = orgs.data[0]["id"]
    else:
        # Crear org de prueba
        org_resp = sb.table("organizations").insert({
            "name": "Test E2E Org",
            "nit": "06140101000001",
            "nrc": "000001-0",
            "plan": "free",
        }).execute()
        org_id = org_resp.data[0]["id"]

    sb.table("users").insert({
        "id": user_id,
        "org_id": org_id,
        "email": TEST_EMAIL,
        "role": "admin",
        "full_name": "Test E2E User",
    }).execute()
    print(f"  ✅ Usuario insertado en tabla users (org: {org_id})")
else:
    org_id = existing.data[0]["org_id"]
    print(f"  ✅ Usuario ya está en tabla users (org: {org_id})")

# Step 1: Login para obtener JWT
print("\n" + "=" * 60)
print("STEP 1: Login → JWT")
print("=" * 60)

anon_client = create_client(SUPABASE_URL, ANON_KEY)
login = anon_client.auth.sign_in_with_password({
    "email": TEST_EMAIL,
    "password": TEST_PASS,
})
token = login.session.access_token
print(f"  ✅ JWT obtenido: {token[:50]}...")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Step 2: GET /api/v1/config/emisor
print("\n" + "=" * 60)
print("STEP 2: GET /api/v1/config/emisor")
print("=" * 60)

r = requests.get(f"{API_URL}/api/v1/config/emisor", headers=headers)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:300]}")

# Step 3: POST /api/v1/config/credentials
print("\n" + "=" * 60)
print("STEP 3: POST /api/v1/config/credentials")
print("=" * 60)

creds_payload = {
    "nit": "06140101000001",
    "nrc": "000001-0",
    "mh_user": "TEST-USER",
    "mh_password": "TEST-PASS",
    "nombre_comercial": "Test E2E SV",
    "razon_social": "Test E2E S.A. de C.V.",
    "actividad_economica": "47190",
    "direccion": "San Salvador, El Salvador",
    "telefono": "22223333",
    "correo": TEST_EMAIL,
    "tipo_establecimiento": "01",
    "codigo_establecimiento": "M001",
    "codigo_punto_venta": "P001",
}

r = requests.post(f"{API_URL}/api/v1/config/credentials", headers=headers, json=creds_payload)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:300]}")

# Step 4: GET /api/v1/dashboard/stats
print("\n" + "=" * 60)
print("STEP 4: GET /api/v1/dashboard/stats")
print("=" * 60)

r = requests.get(f"{API_URL}/api/v1/dashboard/stats", headers=headers)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:300]}")

# Step 5: GET /api/v1/dte/list
print("\n" + "=" * 60)
print("STEP 5: GET /api/v1/dte/list")
print("=" * 60)

r = requests.get(f"{API_URL}/api/v1/dte/list", headers=headers)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:300]}")

print("\n" + "=" * 60)
print("E2E TEST COMPLETE")
print("=" * 60)
print(f"\n  JWT para pruebas manuales:\n  {token}")
