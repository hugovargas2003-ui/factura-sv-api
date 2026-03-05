"""
Circuit Breaker para API del Ministerio de Hacienda
====================================================
Si el MH falla N veces consecutivas, el circuito se "abre" y
manda directo a contingencia sin desperdiciar workers.

Estados:
- CLOSED: funcionando normal, cada request va al MH
- OPEN: MH caído, manda directo a contingencia
- HALF_OPEN: después de cooldown, intenta 1 request al MH

Uso:
    breaker = MHCircuitBreaker()
    if breaker.can_request():
        try:
            response = call_mh(...)
            breaker.record_success()
        except:
            breaker.record_failure()
    else:
        # mandar a contingencia
"""

import time
import logging
from enum import Enum

logger = logging.getLogger("circuit_breaker")


class CircuitState(Enum):
    CLOSED = "closed"        # Normal — requests van al MH
    OPEN = "open"            # MH caído — manda a contingencia
    HALF_OPEN = "half_open"  # Probando si MH volvió


class MHCircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,      # fallos consecutivos para abrir
        recovery_timeout: int = 60,       # segundos antes de probar de nuevo
        success_threshold: int = 2,       # éxitos en half_open para cerrar
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._total_failures = 0
        self._total_short_circuits = 0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info("Circuit breaker → HALF_OPEN (probando MH)")
        return self._state

    def can_request(self) -> bool:
        """¿Se puede enviar request al MH?"""
        current = self.state
        if current == CircuitState.CLOSED:
            return True
        if current == CircuitState.HALF_OPEN:
            return True  # permitir 1 request de prueba
        # OPEN
        self._total_short_circuits += 1
        return False

    def record_success(self) -> None:
        """MH respondió exitosamente."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                logger.info("Circuit breaker → CLOSED (MH recuperado)")
        else:
            self._failure_count = 0

    def record_failure(self) -> None:
        """MH falló (timeout, error, rechazo de conexión)."""
        self._failure_count += 1
        self._total_failures += 1
        self._last_failure_time = time.time()

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                f"Circuit breaker → OPEN (MH caído, {self._failure_count} fallos). "
                f"Mandando a contingencia por {self.recovery_timeout}s"
            )

    def get_status(self) -> dict:
        """Para endpoint /health o admin."""
        return {
            "state": self.state.value,
            "failure_count": self._failure_count,
            "total_failures": self._total_failures,
            "total_short_circuits": self._total_short_circuits,
            "last_failure": self._last_failure_time,
        }


# Singleton — una instancia global para toda la app
mh_breaker = MHCircuitBreaker()
