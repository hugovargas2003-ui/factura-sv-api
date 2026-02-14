"""
FACTURA-SV — Module 1: AuthBridge
Handles authentication against the MH API.

Flow:
1. User provides NIT + Oficina Virtual password
2. We POST to MH /seguridad/auth
3. MH returns a JWT token (valid 24h prod, 48h test)
4. Token is stored in-memory per session (not persisted)

MH Auth Details:
- Endpoint: POST /seguridad/auth
- Body: {"user": NIT, "pwd": password}
- Response: {"status": "OK", "body": {"token": "eyJ...", "roles": [...]}}
- Token type: JWT (HS512)
- Validity: 24h production, 48h test
- Password policy: 13-25 chars, letters+numbers+special, expires every 90 days
"""

import httpx
import logging
from datetime import datetime, timedelta, timezone
from app.core.config import get_mh_url, settings, MHEnvironment

logger = logging.getLogger(__name__)


class AuthBridgeError(Exception):
    """Raised when MH authentication fails."""
    def __init__(self, message: str, status_code: int = 401, mh_response: dict = None):
        self.message = message
        self.status_code = status_code
        self.mh_response = mh_response or {}
        super().__init__(self.message)


class TokenInfo:
    """In-memory token storage for a session."""
    def __init__(self, token: str, nit: str, environment: MHEnvironment):
        self.token = token
        self.nit = nit
        self.environment = environment
        self.obtained_at = datetime.now(timezone.utc)
        # Token validity: 24h prod, 48h test (with safety margin)
        hours = 22 if environment == MHEnvironment.PRODUCTION else 46
        self.expires_at = self.obtained_at + timedelta(hours=hours)

    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) >= self.expires_at

    @property
    def bearer(self) -> str:
        return f"Bearer {self.token}"

    def to_dict(self) -> dict:
        return {
            "nit": self.nit,
            "environment": self.environment.value,
            "obtained_at": self.obtained_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "is_expired": self.is_expired,
            "token_preview": f"{self.token[:20]}...{self.token[-10:]}" if self.token else None,
        }


class AuthBridge:
    """
    Manages authentication with the MH API.

    Each call to authenticate() creates a fresh TokenInfo.
    The token is stored in the session dict in main.py, not here.

    Usage:
        auth = AuthBridge()
        token_info = await auth.authenticate(nit="0614-...", password="...")
        # Use token_info.bearer for subsequent requests
    """

    async def authenticate(self, nit: str, password: str) -> TokenInfo:
        """
        Authenticate against MH API and obtain JWT token.

        Args:
            nit: Contributor NIT (format: 0614-XXXXXX-XXX-X)
            password: Oficina Virtual password (13-25 chars)

        Returns:
            TokenInfo with the JWT token

        Raises:
            AuthBridgeError: If authentication fails
        """
        url = get_mh_url("auth")

        # MH expects these exact field names
        payload = {
            "user": nit,
            "pwd": password,
        }

        logger.info(f"Authenticating NIT {nit[:8]}*** against {url}")

        try:
            async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )

            response_data = response.json() if response.status_code != 500 else {}

            if response.status_code == 200:
                body = response_data.get("body", response_data)
                token = body.get("token")

                if not token:
                    raise AuthBridgeError(
                        message="MH returned 200 but no token in response",
                        status_code=500,
                        mh_response=response_data,
                    )

                token_info = TokenInfo(
                    token=token,
                    nit=nit,
                    environment=settings.mh_environment,
                )

                logger.info(f"Authentication successful for NIT {nit[:8]}***. "
                           f"Token valid until {token_info.expires_at.isoformat()}")

                return token_info

            elif response.status_code == 401:
                raise AuthBridgeError(
                    message="Credenciales inválidas. Verifique su NIT y contraseña de Oficina Virtual.",
                    status_code=401,
                    mh_response=response_data,
                )

            elif response.status_code == 403:
                raise AuthBridgeError(
                    message="Acceso denegado. Su cuenta puede estar bloqueada o la contraseña expiró (caduca cada 90 días).",
                    status_code=403,
                    mh_response=response_data,
                )

            else:
                raise AuthBridgeError(
                    message=f"Error inesperado del MH (HTTP {response.status_code}): "
                            f"{response_data.get('body', response.text[:200])}",
                    status_code=response.status_code,
                    mh_response=response_data,
                )

        except httpx.ConnectError as e:
            raise AuthBridgeError(
                message=f"No se pudo conectar con el MH ({url}). El servidor puede estar fuera de servicio.",
                status_code=502,
            ) from e

        except httpx.TimeoutException as e:
            raise AuthBridgeError(
                message=f"Timeout al conectar con el MH ({url}). Intente nuevamente.",
                status_code=504,
            ) from e

        except AuthBridgeError:
            raise

        except Exception as e:
            logger.exception(f"Unexpected error during authentication: {e}")
            raise AuthBridgeError(
                message=f"Error inesperado durante autenticación: {str(e)}",
                status_code=500,
            ) from e



# Singleton instance
auth_bridge = AuthBridge()
