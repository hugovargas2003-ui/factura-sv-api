"""
FACTURA-SV: Servicio de Emisión DTE (Multi-Tenant SaaS)
========================================================
Orquesta: validar → construir → firmar → transmitir → guardar.
REUTILIZA los módulos existentes:
  - sign_engine (firma JWS con .p12/.pfx via PyJWT)
  - transmit_service (transmisión al MH con reintentos)
  - auth_bridge (autenticación MH)
"""
import logging
from datetime import datetime, timezone

from supabase import Client as SupabaseClient

from app.services.encryption_service import EncryptionService
from app.modules.auth_bridge import auth_bridge, TokenInfo
from app.modules.sign_engine import sign_engine, CertificateSession
from app.modules.transmit_service import transmit_service
from app.modules.invalidation_service import invalidation_service
from app.mh.dte_builder import DTEBuilder, DTE_VERSIONS
from app.schemas.models import InvalidateRequest, TipoResponsable

logger = logging.getLogger("factura-sv.dte_service")


class DTEServiceError(Exception):
    def __init__(self, message: str, code: str = "DTE_ERROR"):
        self.message = message
        self.code = code
        super().__init__(message)


class DTEService:
    """Servicio principal de emisión DTE multi-tenant SaaS."""

    def __init__(self, supabase: SupabaseClient, encryption: EncryptionService):
        self.db = supabase
        self.encryption = encryption
        self._token_cache: dict[str, TokenInfo] = {}

    # ══════════════════════════════════════════════════════════
    # CONFIGURACIÓN
    # ══════════════════════════════════════════════════════════

    async def save_credentials(self, org_id: str, data: dict) -> dict:
        encrypted_pwd = None
        if data.get("mh_password"):
            encrypted_pwd = self.encryption.encrypt_string(data["mh_password"], org_id)

        record = {
            "org_id": org_id,
            "nit": data["nit"],
            "nrc": data["nrc"],
            "nombre": data["nombre"],
            "cod_actividad": data["cod_actividad"],
            "desc_actividad": data["desc_actividad"],
            "nombre_comercial": data.get("nombre_comercial"),
            "tipo_establecimiento": data.get("tipo_establecimiento", "01"),
            "telefono": data["telefono"],
            "correo": data["correo"],
            "direccion_departamento": data["direccion_departamento"],
            "direccion_municipio": data["direccion_municipio"],
            "direccion_complemento": data["direccion_complemento"],
            "codigo_establecimiento": data.get("codigo_establecimiento", "M001"),
            "codigo_punto_venta": data.get("codigo_punto_venta", "P001"),
            "mh_nit_auth": data.get("mh_nit_auth", ""),
            "ambiente": data.get("ambiente", "00"),
            "mh_api_base_url": data.get("mh_api_base_url",
                                         "https://apitest.dtes.mh.gob.sv"),
        }
        if encrypted_pwd:
            record["mh_password_encrypted"] = encrypted_pwd.hex()

        self.db.table("mh_credentials").upsert(
            record, on_conflict="org_id"
        ).execute()

        return {"success": True, "message": "Credenciales guardadas"}

    async def save_certificate(self, org_id: str, cert_bytes: bytes,
                                filename: str) -> dict:
        encrypted_cert = self.encryption.encrypt(cert_bytes, org_id)
        self.db.table("mh_credentials").update({
            "certificate_encrypted": encrypted_cert.hex(),
            "certificate_filename": filename,
        }).eq("org_id", org_id).execute()
        return {"success": True, "message": f"Certificado '{filename}' guardado"}

    async def save_certificate_password(self, org_id: str,
                                         cert_password: str) -> dict:
        encrypted = self.encryption.encrypt_string(cert_password, org_id)
        self.db.table("mh_credentials").update({
            "cert_password_encrypted": encrypted.hex(),
        }).eq("org_id", org_id).execute()
        return {"success": True, "message": "Contraseña de certificado guardada"}

    async def validate_credentials(self, org_id: str) -> dict:
        creds = await self._get_credentials(org_id)
        try:
            await self._authenticate_mh(org_id, creds)
            self.db.table("mh_credentials").update({
                "is_validated": True,
                "last_validated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("org_id", org_id).execute()
            return {"success": True,
                    "message": "Credenciales válidas — conexión MH exitosa"}
        except Exception as e:
            return {"success": False,
                    "message": f"Error de autenticación MH: {str(e)}"}

    async def get_emisor_config(self, org_id: str) -> dict | None:
        result = self.db.table("mh_credentials").select(
            "nit, nrc, nombre, cod_actividad, desc_actividad, "
            "nombre_comercial, tipo_establecimiento, telefono, correo, "
            "direccion_departamento, direccion_municipio, "
            "direccion_complemento, codigo_establecimiento, "
            "codigo_punto_venta, ambiente, mh_api_base_url, "
            "is_validated, last_validated_at, certificate_filename, "
            "created_at, updated_at"
        ).eq("org_id", org_id).maybe_single().execute()
        return result.data if result.data else None

    # ══════════════════════════════════════════════════════════
    # EMISIÓN DTE
    # ══════════════════════════════════════════════════════════

    async def emit_dte(
        self, org_id: str, user_id: str, tipo_dte: str,
        receptor: dict, items: list[dict], *,
        condicion_operacion: int = 1,
        observaciones: str | None = None,
        dte_referencia: dict | None = None,
        dcl_params: dict | None = None,
        cd_params: dict | None = None,
    ) -> dict:
        # 1. Validar credenciales y certificado
        creds = await self._get_credentials(org_id)
        if not creds.get("certificate_encrypted"):
            raise DTEServiceError("Suba su certificado .p12 antes de emitir", "NO_CERT")
        if not creds.get("cert_password_encrypted"):
            raise DTEServiceError("Configure la contraseña de su certificado .p12", "NO_CERT_PWD")

        # 2. Validar cuota mensual
        await self._check_quota(org_id)

        # 3. Obtener número de control atómico
        seq_result = self.db.rpc("get_next_numero_control", {
            "p_org_id": org_id,
            "p_tipo_dte": tipo_dte,
            "p_cod_estab": creds.get("codigo_establecimiento", "M001"),
            "p_cod_pv": creds.get("codigo_punto_venta", "P001"),
        }).execute()
        if not seq_result.data:
            raise DTEServiceError("Error generando número de control", "SEQ_ERROR")
        numero_control = seq_result.data[0]["numero_control"]

        # 4. Construir DTE
        emisor_data = self._creds_to_emisor(creds)
        builder = DTEBuilder(emisor=emisor_data, ambiente=creds.get("ambiente", "00"))
        dte_dict, codigo_gen = builder.build(
            tipo_dte=tipo_dte, numero_control=numero_control,
            receptor=receptor, items=items,
            condicion_operacion=condicion_operacion,
            observaciones=observaciones,
            dte_referencia=dte_referencia,
            dcl_params=dcl_params, cd_params=cd_params,
        )

        # 5. Desencriptar .p12 y cargar en sign_engine
        cert_bytes = self.encryption.decrypt(
            bytes.fromhex(creds["certificate_encrypted"]), org_id)
        cert_pwd = self.encryption.decrypt_string(
            bytes.fromhex(creds["cert_password_encrypted"]), org_id)
        cert_session: CertificateSession = sign_engine.load_certificate(cert_bytes, cert_pwd)

        try:
            # 6. Firmar
            signed_jwt = sign_engine.sign_dte(cert_session, dte_dict)

            # 7. Autenticar con MH
            token_info = await self._authenticate_mh(org_id, creds)

            # 8. Transmitir
            mh_result = await transmit_service.transmit(
                token_info=token_info, signed_dte=signed_jwt,
                tipo_dte=tipo_dte, codigo_generacion=codigo_gen,
            )
        finally:
            cert_session.destroy()

        # 9. Guardar en DB
        estado = "procesado" if mh_result.status == "PROCESADO" else "rechazado"
        resumen = dte_dict.get("resumen", {})
        monto_total = (
            resumen.get("montoTotalOperacion") or resumen.get("totalPagar")
            or resumen.get("totalCompra") or resumen.get("valorTotal") or 0
        )

        dte_record = {
            "org_id": org_id, "tipo_dte": tipo_dte,
            "version": DTE_VERSIONS.get(tipo_dte, 1),
            "numero_control": numero_control,
            "codigo_generacion": codigo_gen,
            "fecha_emision": dte_dict["identificacion"]["fecEmi"],
            "hora_emision": dte_dict["identificacion"]["horEmi"],
            "receptor_tipo": receptor.get("tipo_receptor", "contribuyente"),
            "receptor_nit": receptor.get("num_documento") or receptor.get("nit"),
            "receptor_nrc": receptor.get("nrc"),
            "receptor_nombre": receptor.get("nombre"),
            "receptor_correo": receptor.get("correo"),
            "total_gravada": resumen.get("totalGravada", 0),
            "total_exenta": resumen.get("totalExenta", 0),
            "total_no_sujeta": resumen.get("totalNoSuj", 0),
            "sub_total": resumen.get("subTotal", 0),
            "iva": resumen.get("ivaRete1") or resumen.get("iva", 0),
            "monto_total": monto_total,
            "estado": estado,
            "sello_recibido": mh_result.sello_recepcion,
            "respuesta_mh": mh_result.raw_response,
            "documento_json": dte_dict,
            "documento_jws": signed_jwt,
            "created_by": user_id,
        }
        insert_result = self.db.table("dtes").insert(dte_record).execute()

        logger.info(f"DTE {tipo_dte} emitido: {estado} | {codigo_gen[:8]}...")

        result = {
            "success": mh_result.status == "PROCESADO",
            "dte_id": insert_result.data[0]["id"] if insert_result.data else None,
            "codigo_generacion": codigo_gen,
            "numero_control": numero_control,
            "sello_recibido": mh_result.sello_recepcion,
            "estado": estado,
        }
        if mh_result.status != "PROCESADO":
            result["error"] = mh_result.descripcion_msg
            result["observaciones"] = mh_result.observaciones
        return result

    # ══════════════════════════════════════════════════════════
    # PREVIEW
    # ══════════════════════════════════════════════════════════

    async def preview_dte(self, org_id: str, tipo_dte: str,
                           receptor: dict, items: list[dict], **kwargs) -> dict:
        creds = await self._get_credentials(org_id)
        emisor_data = self._creds_to_emisor(creds)
        builder = DTEBuilder(emisor=emisor_data, ambiente=creds.get("ambiente", "00"))
        dte_dict, _ = builder.build(
            tipo_dte=tipo_dte, numero_control="DTE-XX-PREVIEW-000000000000000",
            receptor=receptor, items=items, **kwargs,
        )
        return dte_dict

    # ══════════════════════════════════════════════════════════
    # INVALIDACIÓN
    # ══════════════════════════════════════════════════════════

    async def invalidar_dte(
        self, org_id: str, user_id: str, dte_id: str,
        tipo_invalidacion: int, motivo: str,
        responsable: dict, solicitante: dict,
    ) -> dict:
        dte_result = self.db.table("dtes").select("*").eq(
            "id", dte_id).eq("org_id", org_id).single().execute()
        if not dte_result.data:
            raise DTEServiceError("DTE no encontrado", "NOT_FOUND")
        dte = dte_result.data
        if dte["estado"] != "procesado":
            raise DTEServiceError("Solo se pueden invalidar DTEs procesados", "INVALID_STATE")

        creds = await self._get_credentials(org_id)

        inv_request = InvalidateRequest(
            codigo_generacion_doc=dte["codigo_generacion"],
            tipo_dte=dte["tipo_dte"],
            motivo=motivo,
            tipo_invalidacion=str(tipo_invalidacion),
            nombre_responsable=responsable["nombre"],
            tipo_documento_responsable=responsable.get("tipo_doc", "36"),
            num_documento_responsable=responsable["num_doc"],
            tipo_responsable=TipoResponsable.EMISOR,
            nit_emisor=creds["nit"],
            nombre_emisor=creds["nombre"],
            nit_receptor=dte.get("receptor_nit", ""),
            nombre_receptor=dte.get("receptor_nombre", ""),
            sello_recibido=dte["sello_recibido"],
            numero_control=dte["numero_control"],
            fecha_emision=str(dte["fecha_emision"]),
            monto_iva=float(dte.get("iva", 0)),
        )

        cert_bytes = self.encryption.decrypt(
            bytes.fromhex(creds["certificate_encrypted"]), org_id)
        cert_pwd = self.encryption.decrypt_string(
            bytes.fromhex(creds["cert_password_encrypted"]), org_id)
        cert_session = sign_engine.load_certificate(cert_bytes, cert_pwd)

        try:
            inv_doc = invalidation_service.build_invalidation_document(request=inv_request)
            token_info = await self._authenticate_mh(org_id, creds)
            mh_result = await invalidation_service.invalidate(
                token_info=token_info, cert_session=cert_session,
                invalidation_doc=inv_doc,
            )
        finally:
            cert_session.destroy()

        import uuid as uuid_mod
        self.db.table("dte_invalidaciones").insert({
            "org_id": org_id, "dte_id": dte_id,
            "tipo_invalidacion": tipo_invalidacion,
            "motivo_invalidacion": motivo,
            "responsable_nombre": responsable["nombre"],
            "responsable_tipo_doc": responsable.get("tipo_doc", "36"),
            "responsable_num_doc": responsable["num_doc"],
            "solicita_nombre": solicitante["nombre"],
            "solicita_tipo_doc": solicitante.get("tipo_doc", "36"),
            "solicita_num_doc": solicitante["num_doc"],
            "codigo_generacion_inv": str(uuid_mod.uuid4()).upper(),
            "sello_recibido": mh_result.sello_invalidacion,
            "estado": "procesado" if mh_result.status == "PROCESADO" else "rechazado",
            "respuesta_mh": mh_result.raw_response,
        }).execute()

        if mh_result.status == "PROCESADO":
            self.db.table("dtes").update({"estado": "invalidado"}).eq("id", dte_id).execute()

        return {
            "success": mh_result.status == "PROCESADO",
            "sello_recibido": mh_result.sello_invalidacion,
            "estado": "invalidado" if mh_result.status == "PROCESADO" else "rechazado",
        }

    # ══════════════════════════════════════════════════════════
    # INTERNOS
    # ══════════════════════════════════════════════════════════

    async def _get_credentials(self, org_id: str) -> dict:
        result = self.db.table("mh_credentials").select("*").eq(
            "org_id", org_id).maybe_single().execute()
        if not result.data:
            raise DTEServiceError("Configure sus credenciales MH antes de emitir", "NO_CREDENTIALS")
        return result.data

    async def _authenticate_mh(self, org_id: str, creds: dict) -> TokenInfo:
        cached = self._token_cache.get(org_id)
        if cached and not cached.is_expired:
            return cached

        nit = creds["mh_nit_auth"] or creds["nit"]
        password = self.encryption.decrypt_string(
            bytes.fromhex(creds["mh_password_encrypted"]), org_id)

        token_info = await auth_bridge.authenticate(nit=nit, password=password)
        self._token_cache[org_id] = token_info
        return token_info

    # Accounts with unlimited access (no quota enforcement)
    BYPASS_EMAILS = {"hugovargas2003@msn.com"}

    async def _check_quota(self, org_id: str):
        """Enforce monthly DTE quota and max_companies limit."""
        # Check if org owner is bypass account
        owner = self.db.table("users").select("email").eq(
            "org_id", org_id).limit(1).execute()
        if owner.data and owner.data[0].get("email") in self.BYPASS_EMAILS:
            return  # Unlimited access

        org_result = self.db.table("organizations").select(
            "monthly_quota, plan, max_companies").eq("id", org_id).single().execute()

        if not org_result.data:
            return

        org = org_result.data
        plan = org.get("plan", "free")
        quota = org.get("monthly_quota", 10)
        max_companies = org.get("max_companies", 1)

        # 1. Check DTE quota (-1 or 999999 = unlimited)
        if quota > 0 and quota < 999999:
            count_result = self.db.rpc("get_monthly_dte_count", {"p_org_id": org_id}).execute()
            monthly_count = count_result.data or 0
            if monthly_count >= quota:
                raise DTEServiceError(
                    f"Límite mensual alcanzado ({monthly_count}/{quota} DTEs). "
                    f"Plan: {plan}. Actualice en /dashboard/planes",
                    "QUOTA_EXCEEDED")

        # 2. Check max companies
        if max_companies > 0 and max_companies < 999:
            creds_count = self.db.table("dte_credentials").select(
                "id", count="exact").eq("org_id", org_id).execute()
            companies_used = creds_count.count or 0
            if companies_used > max_companies:
                raise DTEServiceError(
                    f"Límite de empresas alcanzado ({companies_used}/{max_companies}). "
                    f"Plan: {plan}. Actualice en /dashboard/planes",
                    "COMPANIES_EXCEEDED")

    @staticmethod
    def _creds_to_emisor(creds: dict) -> dict:
        return {
            "nit": creds["nit"], "nrc": creds["nrc"],
            "nombre": creds["nombre"],
            "cod_actividad": creds["cod_actividad"],
            "desc_actividad": creds["desc_actividad"],
            "nombre_comercial": creds.get("nombre_comercial"),
            "tipo_establecimiento": creds.get("tipo_establecimiento", "01"),
            "telefono": creds["telefono"], "correo": creds["correo"],
            "direccion_departamento": creds["direccion_departamento"],
            "direccion_municipio": creds["direccion_municipio"],
            "direccion_complemento": creds["direccion_complemento"],
            "codigo_establecimiento": creds.get("codigo_establecimiento", "M001"),
            "codigo_punto_venta": creds.get("codigo_punto_venta", "P001"),
        }

    async def emit_billing_dte(self, dte_payload: dict, mh_credentials: dict) -> dict:
        """
        Emit a DTE using billing credentials (for auto-invoicing).
        Uses PEM private key directly instead of .p12 certificate.
        """
        import jwt as pyjwt

        tipo_dte = dte_payload["tipo_dte"]
        emisor_data = dte_payload["emisor"]
        receptor = dte_payload["receptor"]
        items = dte_payload["items"]

        # 1. Generate numero_control via sequence RPC
        #    Use Hugo's org for billing sequences
        BILLING_ORG_ID = "35505aeb-7343-4d50-b098-f713239685c3"
        from app.mh.dte_builder import DTEBuilder

        seq_result = self.db.rpc("get_next_numero_control", {
            "p_org_id": BILLING_ORG_ID,
            "p_tipo_dte": tipo_dte,
            "p_cod_estab": "BILL",
            "p_cod_pv": "B001",
        }).execute()
        if not seq_result.data:
            raise DTEServiceError("Error generando número de control para billing", "SEQ_ERROR")
        numero_control = seq_result.data[0]["numero_control"]

        builder = DTEBuilder(emisor=emisor_data, ambiente="01")
        dte_dict, codigo_gen = builder.build(
            tipo_dte=tipo_dte,
            numero_control=numero_control,
            receptor=receptor,
            items=items,
            condicion_operacion=dte_payload.get("condicion_operacion", 1),
        )

        # 2. Sign with PEM private key directly (no .p12 needed)
        pem_key = mh_credentials.get("private_key_pem", "")
        if not pem_key:
            raise DTEServiceError("Clave privada de facturación no configurada", "NO_BILLING_KEY")

        signed_jwt = pyjwt.encode(
            payload=dte_dict,
            key=pem_key,
            algorithm="RS256",
        )

        # 3. Authenticate with MH (billing always uses PRODUCTION)
        nit = mh_credentials["nit"]
        password = mh_credentials["password"]
        import httpx
        from app.modules.auth_bridge import TokenInfo, MHEnvironment
        auth_url = "https://api.dtes.mh.gob.sv/seguridad/auth"
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            auth_resp = await client.post(auth_url, json={"user": nit, "pwd": password},
                headers={"Content-Type": "application/json", "Accept": "application/json"})
        auth_data = auth_resp.json()
        if auth_resp.status_code != 200:
            raise DTEServiceError(f"Billing MH auth failed: {auth_data}", "BILLING_AUTH_ERROR")
        body = auth_data.get("body", auth_data)
        token_info = TokenInfo(token=body["token"], nit=nit, environment=MHEnvironment.PRODUCTION)

        # 4. Transmit to MH (production)
        from app.modules.transmit_service import transmit_service as ts
        mh_result = await ts.transmit(
            token_info=token_info,
            signed_dte=signed_jwt,
            tipo_dte=tipo_dte,
            codigo_generacion=codigo_gen,
        )

        result = {
            "codigo_generacion": codigo_gen,
            "numero_control": dte_dict.get("identificacion", {}).get("numeroControl"),
            "sello_recepcion": mh_result.sello if mh_result.status == "PROCESADO" else None,
            "estado": mh_result.status,
        }

        # 5. Store in billing_invoices table
        try:
            resumen = dte_dict.get("resumen", {})
            monto = (resumen.get("montoTotalOperacion") or resumen.get("totalPagar") or 0)
            self.db.table("billing_invoices").insert({
                "tipo_dte": tipo_dte,
                "codigo_generacion": codigo_gen,
                "numero_control": result["numero_control"],
                "sello_recepcion": result["sello_recepcion"],
                "receptor_nombre": receptor.get("nombre"),
                "receptor_nit": receptor.get("nit"),
                "monto": float(monto),
                "plan_name": items[0].get("descripcion", ""),
                "raw_json": dte_dict,
                "mh_response": {"status": mh_result.status, "sello": mh_result.sello},
                "status": "procesado" if mh_result.status == "PROCESADO" else "error",
            }).execute()
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Failed to store billing invoice: {e}")

        return result
