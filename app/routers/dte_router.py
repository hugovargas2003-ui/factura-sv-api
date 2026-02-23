"""
FACTURA-SV: Router de Emisión DTE
==================================
Endpoints REST para emisión, preview, invalidación,
configuración, catálogos, y dashboard.
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
from fastapi.responses import StreamingResponse
import io

from app.services.import_service import import_productos, import_receptores
from app.services.export_service import fetch_dtes_for_export, generate_xlsx, generate_pdf

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
        generator = DTEPdfGenerator(
            dte_json=dte.get("documento_json", {}),
            sello=dte.get("sello_recibido"),
            estado=dte.get("estado", "desconocido"),
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

    return router
