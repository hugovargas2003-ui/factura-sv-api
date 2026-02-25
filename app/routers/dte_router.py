"""
FACTURA-SV: Router de Emisión DTE
==================================
Endpoints REST para emisión, preview, invalidación,
configuración, catálogos, y dashboard.
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date
import base64
from fastapi.responses import StreamingResponse
import io

from app.services.import_service import import_productos, import_receptores
from app.services.export_service import fetch_dtes_for_export, generate_xlsx, generate_pdf
from app.services import api_key_service
from app.services import fiscal_reports
from app.services import f07_generator
from app.services import org_service
from app.services import contador_service
from app.services import whatsapp_service
from app.services import cxc_service
from app.services import cxp_service
from app.services import batch_service
from app.services import inventory_service
from app.services import contingency_service
from app.services import sucursal_service
from app.services import dashboard_advanced
from app.services.role_guard import require_role, require_admin, require_owner, get_role_permissions

# ── Schemas ──

class CredentialsRequest(BaseModel):
    nit: str = Field(..., example="0614-121271-103-3")
    nrc: str = Field(..., example="1549809")
    nombre: str = Field(..., example="HUGO ERNESTO VARGAS OLIVA")
    cod_actividad: str = Field(..., example="58200")
    desc_actividad: str = Field(..., example="Edicion de programas informaticos")
    nombre_comercial: Optional[str] = None
    tipo_establecimiento: str = "01"
    telefono: str = Field(..., example="00000000")
    correo: str = Field(..., example="hugovargas2003@gmail.com")
    direccion_departamento: str = Field(..., example="06")
    direccion_municipio: str = Field(..., example="14")
    direccion_complemento: str = Field(..., example="San Salvador, El Salvador")
    codigo_establecimiento: str = "M001"
    codigo_punto_venta: str = "P001"
    mh_password: Optional[str] = None
    mh_nit_auth: Optional[str] = None  # NIT para auth API (sin guiones)
    ambiente: str = "00"
    mh_api_base_url: str = "https://apitest.dtes.mh.gob.sv"


class ItemRequest(BaseModel):
    descripcion: str
    precio_unitario: float
    cantidad: float = 1
    tipo_item: int = 2       # 1=Bien, 2=Servicio
    unidad_medida: int = 59  # 59=Unidad
    codigo: Optional[str] = None
    descuento: float = 0
    # Campos específicos por tipo
    tipo_donacion: Optional[int] = None
    valor: Optional[float] = None
    depreciacion: Optional[float] = None
    monto_sujeto: Optional[float] = None
    iva_retenido: Optional[float] = None
    codigo_retencion: Optional[str] = None
    num_documento: Optional[str] = None
    fecha_emision: Optional[str] = None
    tipo_dte_ref: Optional[str] = None


class ReceptorRequest(BaseModel):
    id: Optional[str] = None  # ID del catálogo (para receptor guardado)
    tipo_documento: str = "36"  # 36=NIT, 13=DUI
    num_documento: str
    nrc: Optional[str] = None
    nombre: str
    cod_actividad: Optional[str] = None
    desc_actividad: Optional[str] = None
    nombre_comercial: Optional[str] = None
    tipo_establecimiento: str = "01"
    direccion_departamento: str = "06"
    direccion_municipio: str = "14"
    direccion_complemento: str = "San Salvador"
    telefono: Optional[str] = None
    correo: Optional[str] = None
    tipo_receptor: str = "contribuyente"
    # FEXE
    cod_pais: Optional[str] = None
    tipo_persona: Optional[int] = None
    complemento: Optional[str] = None


class DTEEmitRequest(BaseModel):
    tipo_dte: str = Field(..., example="01")
    receptor: ReceptorRequest
    items: list[ItemRequest]
    condicion_operacion: int = 1
    observaciones: Optional[str] = None
    # Para NC/ND
    dte_referencia_id: Optional[str] = None
    # Para DCL
    dcl_params: Optional[dict] = None
    # Para CD
    cd_params: Optional[dict] = None
    # Sucursal (si multi-sucursal)
    sucursal_id: Optional[str] = None


class InvalidarRequest(BaseModel):
    dte_id: str
    tipo_invalidacion: int = Field(..., ge=1, le=3)
    motivo: str
    responsable_nombre: str
    responsable_tipo_doc: str = "36"
    responsable_num_doc: str
    solicita_nombre: str
    solicita_tipo_doc: str = "36"
    solicita_num_doc: str


class ReceptorCatalogoRequest(BaseModel):
    tipo_documento: str = "36"
    num_documento: str
    nrc: Optional[str] = None
    nombre: str
    cod_actividad: Optional[str] = None
    desc_actividad: Optional[str] = None
    nombre_comercial: Optional[str] = None
    direccion_departamento: Optional[str] = None
    direccion_municipio: Optional[str] = None
    direccion_complemento: Optional[str] = None
    telefono: Optional[str] = None
    correo: Optional[str] = None
    tipo_receptor: str = "contribuyente"
    is_favorite: bool = False


class ProductoCatalogoRequest(BaseModel):
    codigo: Optional[str] = None
    descripcion: str
    precio_unitario: Optional[float] = None
    unidad_medida: int = 59
    tipo_item: int = 2
    tipo_venta: str = "gravada"


# ══════════════════════════════════════════════════════════
# ROUTER FACTORY
# ══════════════════════════════════════════════════════════


class ContingencyRequest(BaseModel):
    motivo: str = Field(..., min_length=5, max_length=500)
    fecha_inicio: str = Field(..., description="Fecha/hora inicio: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
    fecha_fin: str = Field(..., description="Fecha/hora fin: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
    detalle_dte: list = Field(..., description="Lista de DTEs afectados")


def create_dte_router(get_dte_service, get_current_user) -> APIRouter:
    """
    Crea router DTE con inyección de dependencias.

    Args:
        get_dte_service: Dependency que retorna DTEService
        get_current_user: Dependency que retorna {user_id, org_id}
    """
    router = APIRouter(prefix="/api/v1", tags=["DTE"])

    # ── CONFIGURACIÓN ──

    @router.post("/config/credentials")
    async def save_credentials(
        data: CredentialsRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Guardar credenciales MH del emisor."""
        return await service.save_credentials(user["org_id"], data.model_dump())

    @router.post("/config/certificate")
    async def upload_certificate(
        file: UploadFile = File(...),
        password: str = "",
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Subir certificado digital (.p12, .pfx, o .crt CertificadoMH)."""
        if not file.filename:
            raise HTTPException(400, "Archivo requerido")

        fname = file.filename.lower()
        valid_ext = (".p12", ".pfx", ".crt", ".pem", ".cer")
        if not fname.endswith(valid_ext):
            raise HTTPException(400, f"Archivo debe ser {', '.join(valid_ext)}")

        content = await file.read()
        if len(content) > 100_000:
            raise HTTPException(400, "Archivo demasiado grande (máx 100KB)")

        # If .crt/.pem/.cer → could be CertificadoMH XML, auto-convert to .p12
        if fname.endswith((".crt", ".pem", ".cer")):
            try:
                from app.services.cert_converter import convert_mh_cert_to_p12
                p12_bytes, p12_password = convert_mh_cert_to_p12(content)
                # Save converted .p12
                await service.save_certificate(
                    user["org_id"], p12_bytes, file.filename.rsplit(".", 1)[0] + ".p12"
                )
                await service.save_certificate_password(user["org_id"], p12_password)
                return {
                    "success": True,
                    "message": "CertificadoMH convertido a .p12 y guardado automáticamente",
                    "converted": True,
                }
            except Exception as e:
                # Fallback: save raw cert
                await service.save_certificate(user["org_id"], content, file.filename)
                return {"success": True, "message": f"Certificado guardado (sin conversión: {e})"}
        else:
            # .p12 / .pfx → save directly
            await service.save_certificate(user["org_id"], content, file.filename)
            if password:
                await service.save_certificate_password(user["org_id"], password)
            return {"success": True, "message": "Certificado .p12 guardado correctamente"}

    @router.post("/config/validate")
    async def validate_credentials(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Validar credenciales intentando auth con MH."""
        return await service.validate_credentials(user["org_id"])

    @router.post("/config/logo")
    async def upload_logo(
        file: UploadFile = File(...),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Subir logo de la organización para PDFs."""
        if not file.filename:
            raise HTTPException(400, "Archivo requerido")
        fname = file.filename.lower()
        if not fname.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            raise HTTPException(400, "Formato no soportado. Use PNG, JPG o GIF.")
        logo_content = await file.read()
        if len(logo_content) > 500_000:
            raise HTTPException(400, "Imagen demasiado grande (max 500KB)")
        logo_b64 = base64.b64encode(logo_content).decode("utf-8")
        ext = fname.rsplit(".", 1)[-1]
        data_uri = f"data:image/{ext};base64,{logo_b64}"
        service.db.table("mh_credentials").update({
            "logo_base64": data_uri
        }).eq("org_id", user["org_id"]).execute()
        return {"success": True, "message": "Logo guardado", "size_kb": round(len(logo_content) / 1024, 1)}

    @router.get("/config/logo")
    async def get_logo(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Obtener logo de la organización."""
        result = service.db.table("mh_credentials").select(
            "logo_base64"
        ).eq("org_id", user["org_id"]).single().execute()
        if not result.data or not result.data.get("logo_base64"):
            raise HTTPException(404, "No hay logo configurado")
        return {"logo_base64": result.data["logo_base64"]}

    @router.put("/config/pdf-style")
    async def update_pdf_style(
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Actualizar colores del PDF."""
        allowed = {"primary_color", "secondary_color"}
        update = {k: v for k, v in data.items() if k in allowed}
        if not update:
            raise HTTPException(400, "No hay campos validos")
        service.db.table("mh_credentials").update(update).eq(
            "org_id", user["org_id"]).execute()
        return {"success": True, "updated": list(update.keys())}

    @router.get("/config/emisor")
    async def get_emisor(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Obtener configuración del emisor."""
        config = await service.get_emisor_config(user["org_id"])
        if not config:
            raise HTTPException(404, "No hay credenciales configuradas")
        return config

    # ── EMISIÓN DTE ──

    @router.post("/dte/emit")
    async def emit_dte(
        data: DTEEmitRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Emitir un DTE (todos los 13 tipos soportados)."""
        try:
            # Si tiene dte_referencia_id, cargar la referencia
            dte_ref = None
            if data.dte_referencia_id:
                ref_result = service.db.table("dtes").select(
                    "tipo_dte, codigo_generacion, fecha_emision"
                ).eq("id", data.dte_referencia_id).eq(
                    "org_id", user["org_id"]
                ).single().execute()
                if ref_result.data:
                    dte_ref = ref_result.data

            result = await service.emit_dte(
                org_id=user["org_id"],
                user_id=user["user_id"],
                tipo_dte=data.tipo_dte,
                receptor=data.receptor.model_dump(),
                items=[i.model_dump() for i in data.items],
                condicion_operacion=data.condicion_operacion,
                observaciones=data.observaciones,
                dte_referencia=dte_ref,
                dcl_params=data.dcl_params,
                cd_params=data.cd_params,
                sucursal_id=data.sucursal_id,
            )
            return result

        except Exception as e:
            if hasattr(e, "code"):
                raise HTTPException(400, detail={"error": str(e), "code": e.code})
            raise HTTPException(500, detail=str(e))

    @router.post("/dte/preview")
    async def preview_dte(
        data: DTEEmitRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Preview de DTE sin transmitir (para UI)."""
        return await service.preview_dte(
            org_id=user["org_id"],
            tipo_dte=data.tipo_dte,
            receptor=data.receptor.model_dump(),
            items=[i.model_dump() for i in data.items],
            condicion_operacion=data.condicion_operacion,
            observaciones=data.observaciones,
            dcl_params=data.dcl_params,
            cd_params=data.cd_params,
        )

    @router.post("/dte/invalidate")
    async def invalidar(
        data: InvalidarRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Invalidar (anular) un DTE emitido."""
        return await service.invalidar_dte(
            org_id=user["org_id"],
            user_id=user["user_id"],
            dte_id=data.dte_id,
            tipo_invalidacion=data.tipo_invalidacion,
            motivo=data.motivo,
            responsable={
                "nombre": data.responsable_nombre,
                "tipo_doc": data.responsable_tipo_doc,
                "num_doc": data.responsable_num_doc,
            },
            solicitante={
                "nombre": data.solicita_nombre,
                "tipo_doc": data.solicita_tipo_doc,
                "num_doc": data.solicita_num_doc,
            },
        )

    @router.post("/dte/contingencia")
    async def notificar_contingencia(
        data: ContingencyRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Notificar contingencia al MH."""
        # Parse fecha_inicio/fecha_fin into date and time parts
        fi = data.fecha_inicio
        ff = data.fecha_fin
        # Support both "YYYY-MM-DD" and "YYYY-MM-DDTHH:MM:SS" formats
        fec_inicio = fi[:10] if len(fi) >= 10 else fi
        hor_inicio = fi[11:19] if len(fi) > 10 else "00:00:00"
        fec_fin = ff[:10] if len(ff) >= 10 else ff
        hor_fin = ff[11:19] if len(ff) > 10 else "23:59:59"

        return await service.notificar_contingencia(
            org_id=user["org_id"],
            user_id=user["user_id"],
            motivo=data.motivo,
            fecha_inicio=fec_inicio, hora_inicio=hor_inicio,
            fecha_fin=fec_fin, hora_fin=hor_fin,
            detalle_dte=data.detalle_dte,
        )

        # ── CONSULTA DTEs ──

    @router.get("/dte/list")
    async def list_dtes(
        tipo_dte: Optional[str] = None,
        estado: Optional[str] = None,
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        receptor_nit: Optional[str] = None,
        search: Optional[str] = None,
        page: int = Query(1, ge=1),
        per_page: int = Query(20, ge=1, le=100),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar DTEs con filtros y paginación."""
        query = service.db.table("dtes").select(
            "id, tipo_dte, numero_control, codigo_generacion, "
            "fecha_emision, receptor_nombre, receptor_nit, "
            "monto_total, estado, sello_recibido, created_at",
            count="exact"
        ).eq("org_id", user["org_id"]).order("created_at", desc=True)

        if tipo_dte:
            query = query.eq("tipo_dte", tipo_dte)
        if estado:
            query = query.eq("estado", estado)
        if fecha_desde:
            query = query.gte("fecha_emision", str(fecha_desde))
        if fecha_hasta:
            query = query.lte("fecha_emision", str(fecha_hasta))
        if receptor_nit:
            query = query.eq("receptor_nit", receptor_nit)
        if search:
            query = query.or_(
                f"receptor_nombre.ilike.%{search}%,"
                f"numero_control.ilike.%{search}%,"
                f"codigo_generacion.ilike.%{search}%"
            )

        offset = (page - 1) * per_page
        query = query.range(offset, offset + per_page - 1)
        result = query.execute()

        return {
            "data": result.data,
            "total": result.count,
            "page": page,
            "per_page": per_page,
            "total_pages": (result.count + per_page - 1) // per_page if result.count else 0,
        }

    @router.get("/dte/{dte_id}")
    async def get_dte(
        dte_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Detalle completo de un DTE."""
        result = service.db.table("dtes").select("*").eq(
            "id", dte_id
        ).eq("org_id", user["org_id"]).single().execute()

        if not result.data:
            raise HTTPException(404, "DTE no encontrado")
        return result.data

    @router.get("/dte/{dte_id}/pdf")
    async def get_dte_pdf(
        dte_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Generar representación gráfica PDF de un DTE."""
        from fastapi.responses import Response
        from app.services.pdf_generator import DTEPdfGenerator

        result = service.db.table("dtes").select("*").eq(
            "id", dte_id
        ).eq("org_id", user["org_id"]).single().execute()

        if not result.data:
            raise HTTPException(404, "DTE no encontrado")

        dte = result.data
        # Fetch logo if available
        logo_bytes = None
        primary_color = None
        try:
            creds = service.db.table("mh_credentials").select(
                "logo_base64, primary_color"
            ).eq("org_id", user["org_id"]).single().execute()
            if creds.data:
                logo_b64 = creds.data.get("logo_base64")
                if logo_b64 and ";base64," in logo_b64:
                    logo_bytes = base64.b64decode(logo_b64.split(";base64,")[1])
                pc = creds.data.get("primary_color")
                if pc and pc.startswith("#") and len(pc) == 7:
                    primary_color = (int(pc[1:3], 16), int(pc[3:5], 16), int(pc[5:7], 16))
        except Exception:
            pass

        generator = DTEPdfGenerator(
            dte_json=dte.get("documento_json", {}),
            sello=dte.get("sello_recibido"),
            estado=dte.get("estado", "desconocido"),
            logo_bytes=logo_bytes,
            primary_color=primary_color,
        )
        pdf_bytes = generator.generate()

        filename = f"DTE-{dte.get('tipo_dte', 'XX')}-{dte.get('numero_control', '000')}.pdf"
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename={filename}"},
        )

    # ── CATÁLOGO RECEPTORES ──

    @router.get("/receptores")
    async def list_receptores(
        search: Optional[str] = None,
        tipo: Optional[str] = None,
        favorites_only: bool = False,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar receptores del catálogo."""
        query = service.db.table("dte_receptores").select("*").eq(
            "org_id", user["org_id"]
        ).order("uso_count", desc=True)

        if search:
            query = query.or_(f"nombre.ilike.%{search}%,num_documento.ilike.%{search}%")
        if tipo:
            query = query.eq("tipo_receptor", tipo)
        if favorites_only:
            query = query.eq("is_favorite", True)

        return query.limit(50).execute().data

    @router.post("/receptores")
    async def create_receptor(
        data: ReceptorCatalogoRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Crear receptor en catálogo."""
        record = data.model_dump()
        record["org_id"] = user["org_id"]
        return service.db.table("dte_receptores").insert(record).execute().data

    @router.put("/receptores/{receptor_id}")
    async def update_receptor(
        receptor_id: str,
        data: ReceptorCatalogoRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Actualizar receptor."""
        return service.db.table("dte_receptores").update(
            data.model_dump()
        ).eq("id", receptor_id).eq("org_id", user["org_id"]).execute().data

    @router.delete("/receptores/{receptor_id}")
    async def delete_receptor(
        receptor_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Eliminar receptor del catálogo."""
        service.db.table("dte_receptores").delete().eq(
            "id", receptor_id
        ).eq("org_id", user["org_id"]).execute()
        return {"success": True}

    # ── CATÁLOGO PRODUCTOS ──

    @router.get("/productos")
    async def list_productos(
        search: Optional[str] = None,
        active_only: bool = True,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar productos/servicios del catálogo."""
        query = service.db.table("dte_productos").select("*").eq(
            "org_id", user["org_id"]
        ).order("uso_count", desc=True)

        if search:
            query = query.or_(f"descripcion.ilike.%{search}%,codigo.ilike.%{search}%")
        if active_only:
            query = query.eq("is_active", True)

        return query.limit(100).execute().data

    @router.post("/productos")
    async def create_producto(
        data: ProductoCatalogoRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Crear producto/servicio en catálogo."""
        record = data.model_dump()
        record["org_id"] = user["org_id"]
        return service.db.table("dte_productos").insert(record).execute().data

    @router.put("/productos/{producto_id}")
    async def update_producto(
        producto_id: str,
        data: ProductoCatalogoRequest,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Actualizar producto."""
        return service.db.table("dte_productos").update(
            data.model_dump()
        ).eq("id", producto_id).eq("org_id", user["org_id"]).execute().data

    @router.delete("/productos/{producto_id}")
    async def delete_producto(
        producto_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Eliminar producto del catálogo."""
        service.db.table("dte_productos").delete().eq(
            "id", producto_id
        ).eq("org_id", user["org_id"]).execute()
        return {"success": True}

        # ── IMPORT MASIVO ──

    @router.post("/productos/import")
    async def import_productos_csv(
        file: UploadFile = File(...),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Importar productos desde CSV o XLSX."""
        if not file.filename:
            raise HTTPException(400, "No se proporcionó archivo.")
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
        if ext not in ("csv", "xlsx", "xls"):
            raise HTTPException(400, "Formato no soportado. Use .csv o .xlsx")
        content = await file.read()
        if len(content) > 5 * 1024 * 1024:
            raise HTTPException(400, "Archivo excede 5MB.")
        result = await import_productos(content, file.filename, user["org_id"], service.db)
        return result

    @router.post("/receptores/import")
    async def import_receptores_csv(
        file: UploadFile = File(...),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Importar receptores desde CSV o XLSX."""
        if not file.filename:
            raise HTTPException(400, "No se proporcionó archivo.")
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
        if ext not in ("csv", "xlsx", "xls"):
            raise HTTPException(400, "Formato no soportado. Use .csv o .xlsx")
        content = await file.read()
        if len(content) > 5 * 1024 * 1024:
            raise HTTPException(400, "Archivo excede 5MB.")
        result = await import_receptores(content, file.filename, user["org_id"], service.db)
        return result

    # ── EXPORT DTEs ──

    @router.get("/dte/export")
    async def export_dtes(
        format: str = Query("xlsx", pattern="^(xlsx|pdf)$"),
        date_from: Optional[str] = Query(None, alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        date_to: Optional[str] = Query(None, alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        tipo_dte: Optional[str] = Query(None),
        estado: Optional[str] = Query(None),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Exportar historial de DTEs como XLSX o PDF."""
        rows = await fetch_dtes_for_export(
            service.db, user["org_id"],
            date_from=date_from, date_to=date_to,
            tipo_dte=tipo_dte, estado=estado,
        )
        emisor_name = "FACTURA-SV"
        try:
            creds = service.db.table("mh_credentials").select(
                "nombre"
            ).eq("org_id", user["org_id"]).single().execute()
            if creds.data and creds.data.get("nombre"):
                emisor_name = creds.data["nombre"]
        except Exception:
            pass

        if format == "xlsx":
            file_bytes = generate_xlsx(rows, emisor_name)
            filename = f"dtes_{date_from or 'all'}_{date_to or 'all'}.xlsx"
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            file_bytes = generate_pdf(rows, emisor_name)
            filename = f"dtes_{date_from or 'all'}_{date_to or 'all'}.pdf"
            media_type = "application/pdf"

        return StreamingResponse(
            io.BytesIO(file_bytes),
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

        # ── API KEYS (S5-1) ──

    @router.post("/keys")
    async def create_api_key(
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Generar nueva API key para la organización."""
        role = user.get("role", "member")
        if role not in ("admin", "owner"):
            raise HTTPException(403, "Solo administradores pueden crear API keys")
        result = await api_key_service.generate_api_key(
            supabase=service.db,
            org_id=user["org_id"],
            created_by=user["user_id"],
            name=data.get("name", "Default"),
            permissions=data.get("permissions"),
        )
        return result

    @router.get("/keys")
    async def list_api_keys(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar API keys de la organización."""
        return await api_key_service.list_api_keys(service.db, user["org_id"])

    @router.delete("/keys/{key_id}")
    async def revoke_api_key(
        key_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Revocar una API key."""
        role = user.get("role", "member")
        if role not in ("admin", "owner"):
            raise HTTPException(403, "Solo administradores pueden revocar API keys")
        await api_key_service.revoke_api_key(service.db, user["org_id"], key_id)
        return {"success": True, "message": "API key revocada"}

    @router.post("/keys/{key_id}/rotate")
    async def rotate_api_key(
        key_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Rotar API key: revoca la actual y genera una nueva."""
        role = user.get("role", "member")
        if role not in ("admin", "owner"):
            raise HTTPException(403, "Solo administradores pueden rotar API keys")
        result = await api_key_service.rotate_api_key(service.db, user["org_id"], key_id)
        if not result:
            raise HTTPException(404, "API key no encontrada")
        return result

    # ── FISCAL REPORTS (S5-3) ──

    @router.get("/reports/libro-ventas-contribuyente")
    async def libro_ventas_contribuyente(
        year: int = Query(...),
        month: int = Query(..., ge=1, le=12),
        format: str = Query("xlsx"),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Libro de Ventas Contribuyente (CCF tipo 03)."""
        data, filename = await fiscal_reports.generate_libro_ventas_contribuyente(
            service.db, user["org_id"], year, month, format
        )
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if format == "xlsx" else "application/pdf"
        return StreamingResponse(
            io.BytesIO(data),
            media_type=media,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/reports/libro-ventas-consumidor")
    async def libro_ventas_consumidor(
        year: int = Query(...),
        month: int = Query(..., ge=1, le=12),
        format: str = Query("xlsx"),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Libro de Ventas Consumidor Final (Factura tipo 01)."""
        data, filename = await fiscal_reports.generate_libro_ventas_consumidor(
            service.db, user["org_id"], year, month, format
        )
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if format == "xlsx" else "application/pdf"
        return StreamingResponse(
            io.BytesIO(data),
            media_type=media,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/reports/resumen-iva")
    async def resumen_iva(
        year: int = Query(...),
        month: int = Query(..., ge=1, le=12),
        format: str = Query("xlsx"),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Resumen IVA Mensual."""
        data, filename = await fiscal_reports.generate_resumen_iva(
            service.db, user["org_id"], year, month, format
        )
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if format == "xlsx" else "application/pdf"
        return StreamingResponse(
            io.BytesIO(data),
            media_type=media,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # ── INVENTARIO Y KARDEX (T1-03) ──

    @router.get("/inventory")
    async def stock_overview(
        alerts_only: bool = Query(False),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Vista general de inventario con alertas de stock minimo."""
        return await inventory_service.get_stock_overview(
            service.db, user["org_id"], alerts_only=alerts_only
        )

    @router.post("/inventory/{producto_id}/movement")
    async def register_movement(
        producto_id: str,
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Registrar movimiento de inventario (entrada/salida/ajuste)."""
        tipo = data.get("tipo")
        cantidad = data.get("cantidad")
        if not tipo or not cantidad:
            raise HTTPException(400, "tipo y cantidad requeridos")
        try:
            return await inventory_service.register_movement(
                service.db, user["org_id"], producto_id,
                tipo=tipo, cantidad=float(cantidad),
                costo_unitario=float(data.get("costo_unitario", 0)),
                referencia=data.get("referencia", ""),
                created_by=user["user_id"],
            )
        except ValueError as e:
            raise HTTPException(400, str(e))

    @router.get("/inventory/{producto_id}/kardex")
    async def get_kardex(
        producto_id: str,
        fecha_desde: Optional[str] = Query(None),
        fecha_hasta: Optional[str] = Query(None),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Kardex de un producto: historial de movimientos con saldo."""
        try:
            return await inventory_service.get_kardex(
                service.db, user["org_id"], producto_id,
                fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
            )
        except ValueError as e:
            raise HTTPException(404, str(e))

    @router.patch("/productos/{producto_id}/inventory")
    async def toggle_inventory_tracking(
        producto_id: str,
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Activar/configurar tracking de inventario para un producto."""
        update = {}
        if "track_inventory" in data:
            update["track_inventory"] = bool(data["track_inventory"])
        if "stock_minimo" in data:
            update["stock_minimo"] = float(data["stock_minimo"])
        if "stock_actual" in data:
            update["stock_actual"] = float(data["stock_actual"])
        if not update:
            raise HTTPException(400, "Nada que actualizar")
        result = service.db.table("dte_productos").update(update).eq(
            "id", producto_id).eq("org_id", user["org_id"]).execute()
        if not result.data:
            raise HTTPException(404, "Producto no encontrado")
        return {"success": True, "updated": update}

    # ── EMISIÓN MASIVA BATCH (T1-02) ──

    @router.post("/dte/batch/preview")
    async def batch_preview(
        file: UploadFile = File(...),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Preview de emisión masiva: valida CSV/XLSX sin emitir."""
        if not file.filename:
            raise HTTPException(400, "Archivo requerido")
        content = await file.read()
        if len(content) > 5 * 1024 * 1024:
            raise HTTPException(400, "Archivo excede 5MB")
        rows, err = batch_service.parse_batch_file(content, file.filename)
        if err:
            raise HTTPException(400, err)
        if not rows:
            raise HTTPException(400, "Archivo sin datos")
        return batch_service.preview_batch(rows)

    @router.post("/dte/batch/emit")
    async def batch_emit(
        file: UploadFile = File(...),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Emisión masiva: parsea CSV/XLSX y emite DTEs secuencialmente."""
        role = user.get("role", "member")
        if role not in ("admin", "owner", "emisor"):
            raise HTTPException(403, "Sin permisos para emisión masiva")
        if not file.filename:
            raise HTTPException(400, "Archivo requerido")
        content = await file.read()
        if len(content) > 5 * 1024 * 1024:
            raise HTTPException(400, "Archivo excede 5MB")
        rows, err = batch_service.parse_batch_file(content, file.filename)
        if err:
            raise HTTPException(400, err)
        if not rows:
            raise HTTPException(400, "Archivo sin datos")
        if len(rows) > 100:
            raise HTTPException(400, "Maximo 100 DTEs por batch")
        return await batch_service.emit_batch(
            service, user["org_id"], user["user_id"], rows
        )

    @router.get("/dte/batch/template")
    async def batch_template(
        user=Depends(get_current_user),
    ):
        """Retorna las columnas esperadas para el CSV de emisión masiva."""
        return {
            "required": ["tipo_dte", "receptor_tipo_doc", "receptor_num_doc",
                         "receptor_nombre", "item_descripcion", "item_precio", "item_cantidad"],
            "optional": ["receptor_nrc", "receptor_cod_actividad", "receptor_desc_actividad",
                         "receptor_departamento", "receptor_municipio", "receptor_complemento",
                         "receptor_telefono", "receptor_correo",
                         "item_tipo", "item_unidad_medida", "item_codigo",
                         "condicion_operacion", "observaciones"],
            "example_row": {
                "tipo_dte": "03",
                "receptor_tipo_doc": "36",
                "receptor_num_doc": "06141212711033",
                "receptor_nombre": "EMPRESA EJEMPLO S.A.",
                "receptor_nrc": "3319762",
                "receptor_cod_actividad": "46900",
                "receptor_desc_actividad": "Venta al por mayor",
                "item_descripcion": "Servicio de consultoria",
                "item_precio": "100.00",
                "item_cantidad": "1",
                "condicion_operacion": "1",
            },
        }

    # ── CUENTAS POR COBRAR (T1-04) ──

    @router.get("/cxc")
    async def list_cxc(
        estado_pago: Optional[str] = Query(None),
        receptor_nit: Optional[str] = Query(None),
        vencido: Optional[bool] = Query(None),
        page: int = Query(1, ge=1),
        per_page: int = Query(20, ge=1, le=100),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar cuentas por cobrar con filtros."""
        return await cxc_service.get_cxc_list(
            service.db, user["org_id"],
            estado_pago=estado_pago, receptor_nit=receptor_nit,
            vencido=vencido, page=page, per_page=per_page,
        )

    @router.post("/cxc/{dte_id}/pago")
    async def registrar_pago(
        dte_id: str,
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Registrar pago total o parcial de un DTE."""
        monto = data.get("monto")
        if not monto or float(monto) <= 0:
            raise HTTPException(400, "monto requerido y mayor a 0")
        try:
            return await cxc_service.register_payment(
                service.db, user["org_id"], dte_id,
                monto=float(monto),
                metodo=data.get("metodo", "efectivo"),
                referencia=data.get("referencia", ""),
                nota=data.get("nota", ""),
            )
        except ValueError as e:
            raise HTTPException(400, str(e))

    @router.get("/cxc/aging")
    async def aging_report(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Reporte aging CxC (30/60/90 dias)."""
        return await cxc_service.get_aging_report(service.db, user["org_id"])

    @router.get("/cxc/stats")
    async def cxc_stats(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Estadisticas CxC para dashboard."""
        return await cxc_service.get_cxc_stats(service.db, user["org_id"])

    @router.patch("/cxc/{dte_id}/vencimiento")
    async def set_fecha_vencimiento(
        dte_id: str,
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Asignar o cambiar fecha de vencimiento de un DTE."""
        fecha = data.get("fecha_vencimiento")
        if not fecha:
            raise HTTPException(400, "fecha_vencimiento requerida (YYYY-MM-DD)")
        result = service.db.table("dtes").update({
            "fecha_vencimiento": fecha,
        }).eq("id", dte_id).eq("org_id", user["org_id"]).execute()
        if not result.data:
            raise HTTPException(404, "DTE no encontrado")
        return {"success": True, "fecha_vencimiento": fecha}

    # ── MULTI-ORGANIZACIÓN (T1-01) ──

    @router.get("/orgs")
    async def list_my_organizations(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar organizaciones del usuario autenticado."""
        return await org_service.list_user_organizations(
            service.db, user["user_id"]
        )

    @router.post("/orgs/switch")
    async def switch_organization(
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Cambiar organización activa del usuario."""
        org_id = data.get("org_id")
        if not org_id:
            raise HTTPException(400, "org_id requerido")
        try:
            return await org_service.switch_organization(
                service.db, user["user_id"], org_id
            )
        except ValueError as e:
            raise HTTPException(403, str(e))

    @router.post("/orgs/members")
    async def add_org_member(
        data: dict,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Agregar usuario existente a la organización actual."""
        role = user.get("role", "member")
        if role not in ("admin", "owner"):
            raise HTTPException(403, "Solo admin/owner pueden agregar miembros")
        email = data.get("email")
        member_role = data.get("role", "member")
        if not email:
            raise HTTPException(400, "email requerido")
        if member_role not in ("member", "emisor", "auditor", "admin"):
            raise HTTPException(400, "Rol invalido")
        try:
            return await org_service.add_user_to_organization(
                service.db, user["org_id"], email, member_role
            )
        except ValueError as e:
            raise HTTPException(400, str(e))

    @router.delete("/orgs/members/{target_user_id}")
    async def remove_org_member(
        target_user_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Remover usuario de la organización actual."""
        role = user.get("role", "member")
        if role not in ("admin", "owner"):
            raise HTTPException(403, "Solo admin/owner pueden remover miembros")
        try:
            return await org_service.remove_user_from_organization(
                service.db, user["org_id"], target_user_id
            )
        except ValueError as e:
            raise HTTPException(400, str(e))

    @router.get("/orgs/members")
    async def list_org_members(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar miembros de la organización actual."""
        result = service.db.table("user_organizations").select(
            "user_id, role, created_at"
        ).eq("org_id", user["org_id"]).execute()
        members = []
        for m in (result.data or []):
            u = service.db.table("users").select(
                "email, full_name"
            ).eq("id", m["user_id"]).single().execute()
            members.append({
                "user_id": m["user_id"],
                "email": u.data.get("email", "") if u.data else "",
                "full_name": u.data.get("full_name", "") if u.data else "",
                "role": m["role"],
                "created_at": m["created_at"],
            })
        return members

    # ── F-07 ANEXOS DGII (CSV) ──

    @router.get("/reports/f07/anexo1")
    async def f07_anexo1(
        periodo: str = Query(..., description="Formato YYYYMM, ej: 202502", min_length=6, max_length=6),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Anexo 1 F-07 — Ventas a Contribuyentes (CCF, NC, ND). CSV para DGII."""
        csv_bytes = await f07_generator.generate_anexo1(
            service.db, user["org_id"], periodo
        )
        filename = f"Anexo1_Ventas_Contribuyentes_{periodo}.csv"
        return StreamingResponse(
            io.BytesIO(csv_bytes),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/reports/f07/anexo2")
    async def f07_anexo2(
        periodo: str = Query(..., description="Formato YYYYMM, ej: 202502", min_length=6, max_length=6),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Anexo 2 F-07 — Ventas a Consumidor Final (Factura, FSE), agrupado por dia. CSV para DGII."""
        csv_bytes = await f07_generator.generate_anexo2(
            service.db, user["org_id"], periodo
        )
        filename = f"Anexo2_Ventas_ConsumidorFinal_{periodo}.csv"
        return StreamingResponse(
            io.BytesIO(csv_bytes),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/reports/f07/anexo3")
    async def f07_anexo3(
        periodo: str = Query(..., description="YYYY-MM"),
        user=Depends(get_current_user),
    ):
        """Genera Anexo 3 F-07 — Retenciones (tipo 07)."""
        csv_bytes = await f07_generator.generate_anexo3(
            db, user["org_id"], periodo
        )
        return Response(content=csv_bytes, media_type="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="F07_Anexo3_{periodo}.csv"'})

    @router.get("/reports/f07/descargar")
    async def f07_descargar_zip(
        periodo: str = Query(..., description="Formato YYYYMM, ej: 202502", min_length=6, max_length=6),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Descarga ZIP con ambos anexos F-07 (Anexo 1 + Anexo 2)."""
        zip_bytes, filename = await f07_generator.generate_f07_zip(
            service.db, user["org_id"], periodo
        )
        return StreamingResponse(
            io.BytesIO(zip_bytes),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # ── CONTINGENCY QUEUE (S5-4) ──

    @router.get("/contingency")
    async def list_contingency(
        status: str = Query(None),
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Listar DTEs en cola de contingencia."""
        return await contingency_service.list_queue(
            service.db, user["org_id"], status=status
        )

    @router.get("/contingency/stats")
    async def contingency_stats(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Estadísticas de la cola de contingencia."""
        return await contingency_service.get_queue_stats(service.db, user["org_id"])

    @router.post("/contingency/{queue_id}/retry")
    async def retry_contingency(
        queue_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Reintentar un DTE encolado."""
        return await contingency_service.retry_queued_dte(
            service.db, queue_id, user["org_id"]
        )

    @router.delete("/contingency/{queue_id}")
    async def cancel_contingency(
        queue_id: str,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Cancelar un DTE encolado."""
        return await contingency_service.cancel_queued_dte(
            service.db, queue_id, user["org_id"]
        )

    @router.post("/contingency/process")
    async def process_contingency_batch(
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Procesar batch de DTEs encolados."""
        role = user.get("role", "member")
        if role not in ("admin", "owner"):
            raise HTTPException(403, "Solo administradores pueden procesar la cola")
        return await contingency_service.process_queue_batch(
            service.db, user["org_id"], service, user
        )

        # ── DASHBOARD ──

    @router.get("/dashboard/stats")
    async def dashboard_stats(
        fecha_desde: Optional[date] = None,
        fecha_hasta: Optional[date] = None,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Estadísticas para el dashboard."""
        stats = service.db.rpc("get_dte_stats", {
            "p_org_id": user["org_id"],
            "p_from": str(fecha_desde) if fecha_desde else None,
            "p_to": str(fecha_hasta) if fecha_hasta else None,
        }).execute()

        # Cuota mensual
        monthly = service.db.rpc("get_monthly_dte_count", {
            "p_org_id": user["org_id"]
        }).execute()

        org = service.db.table("organizations").select(
            "monthly_quota, plan"
        ).eq("id", user["org_id"]).single().execute()

        return {
            "stats": stats.data[0] if stats.data else {},
            "cuota": {
                "usado": monthly.data or 0,
                "limite": org.data.get("monthly_quota", 50) if org.data else 50,
                "plan": org.data.get("plan", "free") if org.data else "free",
            }
        }


    # ── Sprint 8: Migración Digital ────────────────────────────
    from app.services.extraction_engine import ExtractionEngine
    import tempfile, shutil, os

    @router.post("/import/facturas-fisicas",
                 summary="Extrae datos de facturas PDF/JSON/XML → CSV",
                 tags=["Import/Export"])
    async def import_facturas_fisicas(
        files: List[UploadFile] = File(..., description="Archivos PDF, JSON o XML de facturas"),
        user=Depends(get_current_user),
    ):
        """Sube 1+ facturas (PDF/JSON/XML). Devuelve CSV unificado."""
        org_id = user.get("org_id")
        if not org_id:
            raise HTTPException(status_code=403, detail="Sin organización")

        engine = ExtractionEngine()
        results = []
        for f in files:
            ext = os.path.splitext(f.filename)[1].lower() if "." in f.filename else ""
            if ext not in (".pdf", ".json", ".xml"):
                results.append({"archivo_origen": f.filename, "estado_extraccion": "error", "notas": f"Formato {ext} no soportado"})
                continue
            content = await f.read()
            data = engine.extract_from_bytes(content, f.filename)
            results.append(data)

        if not results:
            raise HTTPException(status_code=400, detail="No se procesaron archivos")

        csv_bytes = engine.results_to_csv(results)
        ok_count = sum(1 for r in results if r.get("estado_extraccion") == "ok")
        return Response(
            content=csv_bytes,
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=facturas_extraidas.csv",
                "X-Total-Processed": str(len(results)),
                "X-Successful": str(ok_count),
                "X-Errors": str(len(results) - ok_count),
            },
        )

    @router.post("/import/facturas-fisicas/preview",
                 summary="Preview extracción (devuelve JSON)",
                 tags=["Import/Export"])
    async def preview_facturas_fisicas(
        files: List[UploadFile] = File(..., description="Archivos PDF, JSON o XML"),
        user=Depends(get_current_user),
    ):
        """Igual que /import/facturas-fisicas pero devuelve JSON."""
        try:
            org_id = user.get("org_id")
            if not org_id:
                raise HTTPException(status_code=403, detail="Sin organización")

            engine = ExtractionEngine()
            results = []
            for f in files:
                ext = os.path.splitext(f.filename)[1].lower() if "." in f.filename else ""
                if ext not in (".pdf", ".json", ".xml"):
                    results.append({"archivo_origen": f.filename, "estado_extraccion": "error", "notas": f"Formato {ext} no soportado"})
                    continue
                content = await f.read()
                data = engine.extract_from_bytes(content, f.filename)
                results.append(data)

            ok_count = sum(1 for r in results if r.get("estado_extraccion") == "ok")
            return {"total_archivos": len(results), "exitosos": ok_count, "errores": len(results) - ok_count, "datos": results}
        except HTTPException:
            raise
        except Exception as e:
            import traceback
            return {"error": str(e), "tb": traceback.format_exc()}

    @router.post("/import/test-upload", tags=["Import/Export"])
    async def test_upload(
        files: List[UploadFile] = File(...),
        user=Depends(get_current_user),
    ):
        """Debug file upload."""
        try:
            results = []
            for f in files:
                content = await f.read()
                engine = ExtractionEngine()
                data = engine.extract_from_bytes(content, f.filename)
                results.append(data)
            ok_count = sum(1 for r in results if r.get("estado_extraccion") == "ok")
            return {"total": len(results), "ok": ok_count, "datos": results}
        except Exception as e:
            import traceback
            return {"error": str(e), "tb": traceback.format_exc()}

    @router.get("/import/test-auth", tags=["Import/Export"])
    async def test_auth(user=Depends(get_current_user)):
        """Test auth."""
        return {"status": "ok", "user": user}

    @router.get("/import/test-extraction", tags=["Import/Export"])
    async def test_extraction():
        """Test endpoint sin auth para verificar motor."""
        try:
            engine = ExtractionEngine()
            import json, tempfile, os
            test_data = {"identificacion": {"tipoDte": "01", "fecEmi": "2026-02-20"}, "emisor": {"nit": "0614", "nombre": "TEST"}, "receptor": {"nombre": "REC"}, "resumen": {"totalPagar": 100}}
            tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
            json.dump(test_data, tmp)
            tmp.close()
            result = engine.extract_from_file(tmp.name)
            os.unlink(tmp.name)
            return {"status": "ok", "result": result}
        except Exception as e:
            import traceback
            return {"status": "error", "error": str(e), "traceback": traceback.format_exc()}



    # ══════════════════════════════════════════════════════════
    # SUCURSALES (T2-01)
    # ══════════════════════════════════════════════════════════

    @router.get("/sucursales")
    async def list_sucursales(user=Depends(get_current_user)):
        return await sucursal_service.list_sucursales(db, user["org_id"])

    @router.get("/sucursales/{sucursal_id}")
    async def get_sucursal(sucursal_id: str, user=Depends(get_current_user)):
        suc = await sucursal_service.get_sucursal(db, user["org_id"], sucursal_id)
        if not suc:
            raise HTTPException(404, "Sucursal no encontrada")
        return suc

    @router.post("/sucursales")
    async def create_sucursal(request: Request, user=Depends(get_current_user)):
        data = await request.json()
        if not data.get("nombre"):
            raise HTTPException(400, "nombre es requerido")
        return await sucursal_service.create_sucursal(db, user["org_id"], data)

    @router.put("/sucursales/{sucursal_id}")
    async def update_sucursal(sucursal_id: str, request: Request, user=Depends(get_current_user)):
        data = await request.json()
        result = await sucursal_service.update_sucursal(db, user["org_id"], sucursal_id, data)
        if not result:
            raise HTTPException(404, "Sucursal no encontrada o sin cambios")
        return result

    @router.delete("/sucursales/{sucursal_id}")
    async def delete_sucursal(sucursal_id: str, user=Depends(get_current_user)):
        result = await sucursal_service.delete_sucursal(db, user["org_id"], sucursal_id)
        if "error" in result:
            raise HTTPException(400, result["error"])
        return result


    # ══════════════════════════════════════════════════════════
    # DASHBOARD AVANZADO (T2-03)
    # ══════════════════════════════════════════════════════════

    @router.get("/dashboard/advanced")
    async def dashboard_advanced_stats(
        dias: int = 30,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        return await dashboard_advanced.get_dashboard_advanced(service.db, user["org_id"], dias)


    # ══════════════════════════════════════════════════════════
    # ROLES Y PERMISOS (T2-04)
    # ══════════════════════════════════════════════════════════

    @router.get("/me/permissions")
    async def my_permissions(user=Depends(get_current_user)):
        """Retorna permisos del usuario actual basado en su rol."""
        return {
            "role": user.get("role", "member"),
            "permissions": get_role_permissions(user.get("role", "member")),
        }

    # ══════════════════════════════════════════════════════════
    # WHATSAPP CLOUD API
    # ══════════════════════════════════════════════════════════

    @router.post("/dte/{dte_id}/whatsapp")
    async def send_dte_whatsapp(
        dte_id: str,
        request: Request,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Enviar PDF del DTE por WhatsApp Cloud API."""
        org_id = user["org_id"]
        # Get WhatsApp config
        wa_config = await whatsapp_service.get_whatsapp_config(db, org_id)
        if not wa_config.get("configured") or not wa_config.get("enabled"):
            raise HTTPException(400, "WhatsApp no está configurado. Vaya a Configuración > WhatsApp.")

        # Get DTE
        dte_result = db.table("dtes").select("*").eq("id", dte_id).eq("org_id", org_id).single().execute()
        if not dte_result.data:
            raise HTTPException(404, "DTE no encontrado")
        dte = dte_result.data

        # Get phone from request body or receptor
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        phone = body.get("phone") or dte.get("receptor_telefono") or ""
        if not phone:
            raise HTTPException(400, "No hay número de teléfono del receptor")

        # Generate PDF
        try:
            from app.services.pdf_generator import DTEPdfGenerator
            pdf_gen = DTEPdfGenerator(
                dte_json=dte.get("documento_json", {}),
                sello=dte.get("sello_recibido", ""),
                estado=dte.get("estado", ""),
            )
            pdf_bytes = pdf_gen.generate()
        except Exception as e:
            raise HTTPException(500, f"Error generando PDF: {e}")

        # Get decrypted access token
        creds = db.table("dte_credentials").select(
            "whatsapp_phone_number_id, whatsapp_access_token_encrypted"
        ).eq("org_id", org_id).single().execute()

        if not creds.data or not creds.data.get("whatsapp_access_token_encrypted"):
            raise HTTPException(400, "Token de WhatsApp no configurado")

        encryption = service.encryption
        access_token = encryption.decrypt_string(creds.data["whatsapp_access_token_encrypted"], org_id)

        result = await whatsapp_service.send_dte_pdf(
            phone_number_id=creds.data["whatsapp_phone_number_id"],
            access_token=access_token,
            recipient_phone=phone,
            pdf_bytes=pdf_bytes,
            filename=f"DTE_{dte.get('numero_control', 'doc')}.pdf",
            caption=f"DTE {dte.get('numero_control', '')} - {dte.get('receptor_nombre', '')}",
        )
        return result

    @router.get("/config/whatsapp")
    async def get_whatsapp_config(user=Depends(get_current_user)):
        """Obtener configuración WhatsApp de la org."""
        return await whatsapp_service.get_whatsapp_config(db, user["org_id"])

    @router.post("/config/whatsapp")
    async def save_whatsapp_config_endpoint(
        request: Request,
        service=Depends(get_dte_service),
        user=Depends(get_current_user),
    ):
        """Guardar configuración WhatsApp."""
        data = await request.json()
        return await whatsapp_service.save_whatsapp_config(
            db, user["org_id"], service.encryption, data
        )

    # ══════════════════════════════════════════════════════════
    # PANEL CONTADOR (cross-org dashboard)
    # ══════════════════════════════════════════════════════════

    @router.get("/contador/dashboard")
    async def contador_dashboard(user=Depends(get_current_user)):
        """Dashboard consolidado: stats de TODAS las orgs del usuario."""
        try:
            result = await contador_service.get_contador_dashboard(
                db, user["user_id"]
            )
            return result
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    @router.get("/contador/report")
    async def contador_report(
        fecha_desde: str = Query(..., description="YYYY-MM-DD"),
        fecha_hasta: str = Query(..., description="YYYY-MM-DD"),
        user=Depends(get_current_user),
    ):
        """Reporte consolidado cross-org por rango de fechas."""
        try:
            result = await contador_service.get_cross_org_report(
                db, user["user_id"], fecha_desde, fecha_hasta
            )
            return result
        except Exception as e:
            raise HTTPException(500, detail=str(e))

    @router.post("/contador/add-client")
    async def contador_add_client(
        request: Request,
        user=Depends(get_current_user),
    ):
        """Crear nueva org cliente y vincular al contador como owner."""
        data = await request.json()
        if not data.get("nombre"):
            raise HTTPException(400, "El nombre de la empresa es requerido")
        try:
            result = await contador_service.add_client_org(
                db, user["user_id"], user["org_id"], data
            )
            return result
        except ValueError as e:
            code = 402 if "plan" in str(e).lower() else 400
            raise HTTPException(code, detail=str(e))
        except Exception as e:
            raise HTTPException(500, detail=str(e))


    # ── Cuentas por Pagar (CxP) ──────────────────────────────────

    @router.get("/cxp")
    async def list_cxp(
        estado_pago: str = None, proveedor: str = None,
        vencido: bool = None, page: int = 1, per_page: int = 20,
        service=Depends(get_dte_service), user=Depends(get_current_user),
    ):
        """Listar cuentas por pagar con filtros."""
        return await cxp_service.list_cxp(
            service.db, user["org_id"], estado_pago, proveedor, vencido, page, per_page
        )

    @router.post("/cxp")
    async def create_cxp(
        body: dict,
        service=Depends(get_dte_service), user=Depends(get_current_user),
    ):
        """Crear nueva cuenta por pagar."""
        try:
            return await cxp_service.create_cxp(service.db, user["org_id"], user["user_id"], body)
        except ValueError as e:
            raise HTTPException(400, detail=str(e))

    @router.post("/cxp/{cxp_id}/pago")
    async def register_cxp_payment(
        cxp_id: str, body: dict,
        service=Depends(get_dte_service), user=Depends(get_current_user),
    ):
        """Registrar pago en cuenta por pagar."""
        try:
            return await cxp_service.register_payment(
                service.db, user["org_id"], cxp_id,
                monto=body["monto"], metodo=body.get("metodo", "efectivo"),
                referencia=body.get("referencia", ""), nota=body.get("nota", ""),
            )
        except ValueError as e:
            raise HTTPException(400, detail=str(e))

    @router.delete("/cxp/{cxp_id}")
    async def delete_cxp(
        cxp_id: str,
        service=Depends(get_dte_service), user=Depends(get_current_user),
    ):
        """Eliminar cuenta por pagar (solo si no tiene pagos)."""
        try:
            return await cxp_service.delete_cxp(service.db, user["org_id"], cxp_id)
        except ValueError as e:
            raise HTTPException(400, detail=str(e))

    @router.get("/cxp/aging")
    async def cxp_aging(
        service=Depends(get_dte_service), user=Depends(get_current_user),
    ):
        """Reporte de antigüedad de cuentas por pagar."""
        return await cxp_service.get_aging_report(service.db, user["org_id"])

    @router.get("/cxp/stats")
    async def cxp_stats(
        service=Depends(get_dte_service), user=Depends(get_current_user),
    ):
        """Estadísticas de cuentas por pagar."""
        return await cxp_service.get_cxp_stats(service.db, user["org_id"])

    return router

