"""
Motor de Extracción de Facturas - FACTURA-SV
=============================================
Pipeline:
1. Detectar tipo de archivo (JSON, XML, PDF)
2. Extraer usando método específico
3. Si falla PDF, intentar OCR + AI fallback
4. Devolver estructura unificada (diccionario)
5. Exportar a CSV

Uso como servicio:
    engine = ExtractionEngine()
    results = engine.extract_batch(file_paths)
    csv_bytes = engine.results_to_csv(results)
"""

import os
import json
import re
import xml.etree.ElementTree as ET
import csv
import io
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# Imports opcionales — degradación elegante
try:
    import pdfplumber
    PDFPLUMBER_OK = True
except ImportError:
    PDFPLUMBER_OK = False
    logger.warning("pdfplumber no instalado. Extracción PDF limitada.")

try:
    import pytesseract
    from PIL import Image
    OCR_OK = True
except ImportError:
    OCR_OK = False
    logger.warning("pytesseract/Pillow no instalados. OCR no disponible.")

try:
    from openai import OpenAI
    OPENAI_OK = True
except ImportError:
    OPENAI_OK = False

try:
    from anthropic import Anthropic
    ANTHROPIC_OK = True
except ImportError:
    ANTHROPIC_OK = False


class ExtractionError(Exception):
    pass


# Campos estándar del CSV de salida
CSV_COLUMNS = [
    "archivo_origen",
    "tipo_dte",
    "nit_emisor",
    "nombre_emisor",
    "nit_receptor",
    "nombre_receptor",
    "fecha",
    "numero_control",
    "codigo_generacion",
    "subtotal",
    "iva",
    "total",
    "condicion_pago",
    "items_json",
    "estado_extraccion",
    "notas",
]


class ExtractionEngine:
    """Motor principal de extracción multi-formato."""

    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
        use_ai_fallback: bool = True,
    ):
        self.use_ai_fallback = use_ai_fallback
        self.ai_client = None
        self.ai_provider = None

        # Preferir Anthropic (Claude) si está disponible
        if use_ai_fallback and ANTHROPIC_OK and anthropic_api_key:
            self.ai_client = Anthropic(api_key=anthropic_api_key)
            self.ai_provider = "anthropic"
        elif use_ai_fallback and OPENAI_OK and openai_api_key:
            self.ai_client = OpenAI(api_key=openai_api_key)
            self.ai_provider = "openai"

        if use_ai_fallback and not self.ai_client:
            logger.warning("AI fallback solicitado pero ningún proveedor configurado.")

    # ── Método principal ──────────────────────────────────────

    def extract_from_file(self, file_path: str) -> Dict[str, Any]:
        """Detecta tipo y extrae campos."""
        ext = os.path.splitext(file_path)[1].lower()
        filename = os.path.basename(file_path)
        result = {"archivo_origen": filename, "estado_extraccion": "ok", "notas": ""}

        try:
            if ext == ".json":
                data = self._extract_json(file_path)
            elif ext == ".xml":
                data = self._extract_xml(file_path)
            elif ext == ".pdf":
                data = self._extract_pdf(file_path)
            else:
                raise ExtractionError(f"Formato no soportado: {ext}")

            result.update(data)

        except ExtractionError as e:
            result["estado_extraccion"] = "error"
            result["notas"] = str(e)
        except Exception as e:
            result["estado_extraccion"] = "error"
            result["notas"] = f"Error inesperado: {str(e)}"

        return result

    def extract_from_bytes(self, content: bytes, filename: str) -> Dict[str, Any]:
        """Extrae desde bytes en memoria (para uso desde API)."""
        import tempfile
        ext = os.path.splitext(filename)[1].lower()
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        try:
            return self.extract_from_file(tmp_path)
        finally:
            os.unlink(tmp_path)

    def extract_batch(self, file_paths: List[str]) -> List[Dict[str, Any]]:
        """Procesa múltiples archivos."""
        results = []
        for fp in file_paths:
            r = self.extract_from_file(fp)
            results.append(r)
            logger.info(f"  [{r['estado_extraccion']}] {fp}")
        return results

    def results_to_csv(self, results: List[Dict[str, Any]]) -> bytes:
        """Convierte resultados a CSV en bytes (UTF-8 BOM)."""
        df = pd.DataFrame(results)
        # Asegurar que todas las columnas existan
        for col in CSV_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[CSV_COLUMNS]
        buf = io.BytesIO()
        df.to_csv(buf, index=False, encoding="utf-8-sig")
        return buf.getvalue()

    # ── Extractores por formato ───────────────────────────────

    def _extract_json(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return self._map_dte_fields(data)

    def _extract_xml(self, file_path: str) -> Dict[str, Any]:
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = self._xml_to_dict(root)
        return self._map_dte_fields(data)

    def _extract_pdf(self, file_path: str) -> Dict[str, Any]:
        # Paso 1: Intentar texto seleccionable
        text = self._pdf_to_text(file_path)
        if text and self._has_invoice_markers(text):
            data = self._parse_text_regex(text)
            if data.get("total"):
                data["notas"] = "Extraído con pdfplumber (texto directo)"
                return data

        # Paso 2: OCR
        if OCR_OK:
            text = self._ocr_pdf(file_path)
            if text:
                # Paso 2a: Intentar regex sobre OCR
                data = self._parse_text_regex(text)
                if data.get("total"):
                    data["notas"] = "Extraído con OCR + regex"
                    return data

                # Paso 2b: AI fallback
                if self.ai_client:
                    data = self._extract_with_ai(text)
                    data["notas"] = f"Extraído con OCR + {self.ai_provider}"
                    return data

        # Paso 3: Solo AI sobre texto plano
        if text and self.ai_client:
            data = self._extract_with_ai(text)
            data["notas"] = f"Extraído con {self.ai_provider} (sin OCR)"
            return data

        raise ExtractionError(
            "No se pudo extraer: instale pdfplumber/tesseract o configure AI."
        )

    # ── Helpers PDF ───────────────────────────────────────────

    def _pdf_to_text(self, file_path: str) -> str:
        if not PDFPLUMBER_OK:
            return ""
        text = ""
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        text += t + "\n"
        except Exception as e:
            logger.error(f"pdfplumber error: {e}")
        return text

    def _ocr_pdf(self, file_path: str) -> str:
        """OCR con Tesseract. Requiere pdf2image + poppler."""
        try:
            from pdf2image import convert_from_path
            import numpy as np
        except ImportError:
            logger.warning("pdf2image no instalado. OCR no disponible.")
            return ""

        try:
            images = convert_from_path(file_path, dpi=300)
        except Exception as e:
            logger.error(f"pdf2image error: {e}")
            return ""

        full_text = ""
        for img in images:
            # Preprocesamiento: escala de grises + umbral
            img_gray = img.convert("L")
            img_np = np.array(img_gray)
            threshold = 128
            img_np = ((img_np > threshold) * 255).astype(np.uint8)
            img_clean = Image.fromarray(img_np)
            try:
                text = pytesseract.image_to_string(img_clean, lang="spa")
                full_text += text + "\n"
            except Exception as e:
                logger.error(f"Tesseract error: {e}")
        return full_text

    def _has_invoice_markers(self, text: str) -> bool:
        score = 0
        if re.search(r"NIT|N\.I\.T", text, re.IGNORECASE):
            score += 1
        if re.search(r"\d{2}[/-]\d{2}[/-]\d{4}", text):
            score += 1
        if re.search(r"total|monto|pagar", text, re.IGNORECASE):
            score += 1
        if re.search(r"factura|crédito fiscal|DTE|comprobante", text, re.IGNORECASE):
            score += 1
        return score >= 2

    # ── Parser regex (facturas salvadoreñas) ──────────────────

    def _parse_text_regex(self, text: str) -> Dict[str, Any]:
        data = {}

        # NIT emisor (formato: 0614-121271-103-3 o 12345678-9)
        nit = re.search(r"(\d{4}-\d{6}-\d{3}-\d)", text)
        if not nit:
            nit = re.search(r"(\d{8,9}-\d)", text)
        if nit:
            data["nit_emisor"] = nit.group(1)

        # Fecha (dd/mm/yyyy, dd-mm-yyyy, yyyy-mm-dd)
        fecha = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4})", text)
        if fecha:
            data["fecha"] = fecha.group(1)
        else:
            fecha = re.search(r"(\d{4}-\d{2}-\d{2})", text)
            if fecha:
                data["fecha"] = fecha.group(1)

        # Número de control / factura
        num = re.search(
            r"(DTE-\d+-[A-Z0-9]+-\d+|N(?:o|°|úmero)?\s*(?:de )?Control\s*:?\s*([\w-]+))",
            text,
            re.IGNORECASE,
        )
        if num:
            data["numero_control"] = num.group(2) if num.group(2) else num.group(1)

        # Tipo DTE
        if re.search(r"crédito fiscal|CCF", text, re.IGNORECASE):
            data["tipo_dte"] = "03"
        elif re.search(r"sujeto excluido", text, re.IGNORECASE):
            data["tipo_dte"] = "14"
        elif re.search(r"nota de crédito", text, re.IGNORECASE):
            data["tipo_dte"] = "05"
        elif re.search(r"nota de débito", text, re.IGNORECASE):
            data["tipo_dte"] = "06"
        elif re.search(r"retención", text, re.IGNORECASE):
            data["tipo_dte"] = "07"
        elif re.search(r"exportación", text, re.IGNORECASE):
            data["tipo_dte"] = "11"
        elif re.search(r"donación", text, re.IGNORECASE):
            data["tipo_dte"] = "15"
        elif re.search(r"factura", text, re.IGNORECASE):
            data["tipo_dte"] = "01"

        # Total
        total = re.search(
            r"total\s*(?:a\s*pagar)?\s*:?\s*\$?\s*([\d,]+\.\d{2})",
            text,
            re.IGNORECASE,
        )
        if total:
            data["total"] = float(total.group(1).replace(",", ""))

        # Sub-total
        sub = re.search(
            r"sub\s*-?total\s*:?\s*\$?\s*([\d,]+\.\d{2})", text, re.IGNORECASE
        )
        if sub:
            data["subtotal"] = float(sub.group(1).replace(",", ""))

        # IVA
        iva = re.search(
            r"IVA\s*(?:13%)?\s*:?\s*\$?\s*([\d,]+\.\d{2})", text, re.IGNORECASE
        )
        if iva:
            data["iva"] = float(iva.group(1).replace(",", ""))

        # Nombre emisor (línea después de NIT generalmente)
        nombre = re.search(
            r"(?:Razón Social|Nombre|Emisor)\s*:?\s*(.+)", text, re.IGNORECASE
        )
        if nombre:
            data["nombre_emisor"] = nombre.group(1).strip()[:100]

        return data

    # ── Mapeo DTE estándar (JSON/XML) ─────────────────────────

    def _map_dte_fields(self, data: Dict) -> Dict[str, Any]:
        result = {}

        # Identificación
        ident = data.get("identificacion", {})
        result["tipo_dte"] = ident.get("tipoDte") or data.get("tipoDte", "")
        result["numero_control"] = ident.get("numeroControl") or data.get("numeroControl", "")
        result["codigo_generacion"] = ident.get("codigoGeneracion") or data.get("codigoGeneracion", "")
        result["fecha"] = ident.get("fecEmi") or data.get("fecEmi", "")

        # Emisor
        emisor = data.get("emisor", {})
        result["nit_emisor"] = emisor.get("nit") or data.get("nit", "")
        result["nombre_emisor"] = emisor.get("nombre") or data.get("nombre_emisor", "")

        # Receptor
        receptor = data.get("receptor", {})
        result["nit_receptor"] = receptor.get("numDocumento") or receptor.get("nit", "")
        result["nombre_receptor"] = receptor.get("nombre") or data.get("nombre_receptor", "")

        # Resumen
        resumen = data.get("resumen", {})
        result["subtotal"] = resumen.get("subTotal") or resumen.get("totalNoSuj", 0)
        result["iva"] = resumen.get("totalIva") or resumen.get("ivaRete1", 0)
        result["total"] = (
            resumen.get("totalPagar")
            or resumen.get("montoTotalOperacion")
            or data.get("total", 0)
        )

        # Condición de pago
        result["condicion_pago"] = resumen.get("condicionOperacion", "")

        # Items
        items = data.get("cuerpoDocumento", [])
        if items:
            result["items_json"] = json.dumps(items, ensure_ascii=False)

        return result

    # ── AI extraction fallback ────────────────────────────────

    def _extract_with_ai(self, text: str) -> Dict[str, Any]:
        """Usa Claude o GPT para extraer campos del texto."""
        prompt = (
            "Extrae los siguientes datos de esta factura salvadoreña. "
            "Responde SOLO con JSON válido, sin markdown, sin explicaciones.\n"
            "Campos:\n"
            '  "tipo_dte": código (01=Factura, 03=CCF, 05=NC, 06=ND, 07=Retención, 11=Export, 14=Suj.Excl, 15=Donación)\n'
            '  "nit_emisor": NIT del emisor\n'
            '  "nombre_emisor": nombre/razón social emisor\n'
            '  "nit_receptor": NIT/DUI del receptor\n'
            '  "nombre_receptor": nombre receptor\n'
            '  "fecha": fecha emisión formato YYYY-MM-DD\n'
            '  "numero_control": número de control\n'
            '  "subtotal": monto sin IVA (número)\n'
            '  "iva": monto IVA (número)\n'
            '  "total": monto total (número)\n'
            '  "condicion_pago": "1"=contado, "2"=crédito\n'
            "Si un campo no se encuentra, usa null.\n\n"
            f"TEXTO DE LA FACTURA:\n{text[:4000]}"
        )

        try:
            if self.ai_provider == "anthropic":
                resp = self.ai_client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = resp.content[0].text
            else:
                resp = self.ai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Eres un extractor de datos de facturas."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0,
                )
                raw = resp.choices[0].message.content

            # Limpiar JSON
            raw = raw.strip()
            if raw.startswith("```"):
                raw = re.sub(r"```(?:json)?\n?", "", raw).rstrip("`").strip()
            return json.loads(raw)

        except Exception as e:
            logger.error(f"AI extraction error: {e}")
            return {"notas": f"AI falló: {str(e)}"}

    # ── Helpers ────────────────────────────────────────────────

    def _xml_to_dict(self, element) -> Dict:
        result = {}
        for child in element:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if len(child) > 0:
                result[tag] = self._xml_to_dict(child)
            else:
                result[tag] = child.text
        return result
