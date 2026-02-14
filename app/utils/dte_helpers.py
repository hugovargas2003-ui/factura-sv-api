"""
FACTURA-SV — DTE Utilities
Helper functions for generating DTE identifiers and validation.
"""

import uuid
from datetime import datetime, timezone


def generate_codigo_generacion() -> str:
    """
    Generate a UUID v4 for DTE codigoGeneracion.
    Format: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX (uppercase)
    MH requires 36 characters.
    """
    return str(uuid.uuid4()).upper()


def generate_numero_control(
    tipo_dte: str,
    establecimiento: str = "M001",
    punto_venta: str = "P001",
    correlativo: int = 1,
) -> str:
    """
    Generate DTE número de control.
    Format: DTE-TT-SSSS-PPPP-NNNNNNNNNNNNNNN
    - TT: tipo DTE (01, 03, etc.)
    - SSSS: código establecimiento (M001, S001, etc.)
    - PPPP: código punto de venta (P001, etc.)
    - NNNNNNNNNNNNNNN: correlativo (15 digits, zero-padded, resets yearly)
    Total: 32 characters

    Reference: Normativa de Cumplimiento DTE, sección 7.2 "Número de Control"
    """
    return f"DTE-{tipo_dte}-{establecimiento}-{punto_venta}-{str(correlativo).zfill(15)}"


def current_sv_datetime() -> tuple[str, str]:
    """
    Get current date and time in El Salvador (UTC-6) format.
    Returns: (fecha "YYYY-MM-DD", hora "HH:MM:SS")
    """
    from datetime import timedelta
    sv_time = datetime.now(timezone.utc) - timedelta(hours=6)
    return sv_time.strftime("%Y-%m-%d"), sv_time.strftime("%H:%M:%S")


def validate_nit(nit: str) -> bool:
    """
    Basic NIT format validation.
    Expected format: XXXX-XXXXXX-XXX-X (14 digits + 3 hyphens)
    """
    parts = nit.split("-")
    if len(parts) != 4:
        return False
    lengths = [4, 6, 3, 1]
    return all(
        len(part) == length and part.isdigit()
        for part, length in zip(parts, lengths)
    )


def validate_nrc(nrc: str) -> bool:
    """
    Basic NRC format validation.
    Expected format: XXXXXX-X (digits-check digit)
    """
    parts = nrc.split("-")
    if len(parts) != 2:
        return False
    return parts[0].isdigit() and parts[1].isdigit() and len(parts[1]) == 1
