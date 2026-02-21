import requests

API_URL = "https://factura-sv-api-production.up.railway.app"
TOKEN = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIyMDA2YjI2LTYwYWItNGQ4My04ZDRkLTY5Mjk2YWNhYjVmMCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2ZjcWV2bXVqZGpmYmRjeWpkbXpnLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiI4M2ZkMDk1ZC03MGU4LTRiZjQtYTBjNi05ZTNkMzU2OGExZWIiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzcxNjQzNzkwLCJpYXQiOjE3NzE2NDAxOTAsImVtYWlsIjoidGVzdC1lMmVAZmFjdHVyYS1zdi5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7ImVtYWlsX3ZlcmlmaWVkIjp0cnVlfSwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJwYXNzd29yZCIsInRpbWVzdGFtcCI6MTc3MTY0MDE5MH1dLCJzZXNzaW9uX2lkIjoiZjhkNDcyYzItM2Y1MC00NzhkLWI3ZDktMDc3NmU3MWMyMTYzIiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.p7eekxLXDz_T5jmJMfV2sWWnIqqnwV-8RgbFTwai3Fu_QN-1TIVIGkFD1lFHAQxK1USmEzR3dxrm8MMfqP40FQ"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# Step 1: POST credentials con campos correctos
print("=" * 60)
print("STEP 1: POST /api/v1/config/credentials")
print("=" * 60)

creds = {
    "nit": "06141211271033",
    "nrc": "1549809",
    "nombre": "HUGO ERNESTO VARGAS OLIVA",
    "cod_actividad": "58200",
    "desc_actividad": "Edicion de programas informaticos",
    "nombre_comercial": "FACTURA-SV Test",
    "tipo_establecimiento": "01",
    "telefono": "22223333",
    "correo": "test-e2e@factura-sv.com",
    "direccion_departamento": "06",
    "direccion_municipio": "14",
    "direccion_complemento": "San Salvador, El Salvador",
    "codigo_establecimiento": "M001",
    "codigo_punto_venta": "P001",
    "mh_password": "TEST-PASS-123",
    "mh_nit_auth": "06141211271033",
    "ambiente": "00",
    "mh_api_base_url": "https://apitest.dtes.mh.gob.sv",
}

r = requests.post(f"{API_URL}/api/v1/config/credentials", headers=headers, json=creds)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:500]}")

# Step 2: GET emisor (ahora debería funcionar)
print("\n" + "=" * 60)
print("STEP 2: GET /api/v1/config/emisor")
print("=" * 60)

r = requests.get(f"{API_URL}/api/v1/config/emisor", headers=headers)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:500]}")

# Step 3: Dashboard stats
print("\n" + "=" * 60)
print("STEP 3: GET /api/v1/dashboard/stats")
print("=" * 60)

r = requests.get(f"{API_URL}/api/v1/dashboard/stats", headers=headers)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:500]}")

print("\n" + "=" * 60)
print("E2E TEST ROUND 2 COMPLETE")
print("=" * 60)
