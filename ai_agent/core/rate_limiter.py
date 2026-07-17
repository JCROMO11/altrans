import asyncio
import time
from collections import defaultdict, deque
from typing import Optional

from loguru import logger


class RateLimiter:
    """Control de concurrencia + ventana deslizante por usuario.

    Garantiza que un usuario nunca tenga 2 requests en paralelo (lock)
    y limita la tasa a max_per_minute mensajes por minuto (ventana deslizante).

    Si un mensaje llega mientras el usuario está siendo procesado, se encola
    y se procesa automáticamente al terminar el mensaje actual.
    """

    def __init__(self, max_per_minute: int = 5):
        self._max_per_minute = max_per_minute
        self._sliding_window: dict[str, deque[float]] = defaultdict(lambda: deque())
        self._locks: dict[str, asyncio.Lock] = {}
        self._queues: dict[str, deque] = defaultdict(deque)
        self._global_lock = asyncio.Lock()

    async def try_acquire(self, user_id: str, message_data: dict) -> tuple[bool, str]:
        """Intenta adquirir el slot de procesamiento para un usuario.

        Devuelve (ok, reason):
          ok=True, 'process'      — procesar este mensaje ahora
          ok=False, 'queued'      — usuario ocupado, mensaje encolado
          ok=False, 'rate_limited' — excedió el límite por minuto
        """
        async with self._global_lock:
            now = time.monotonic()
            window = self._sliding_window[user_id]
            while window and now - window[0] > 60:
                window.popleft()

            if len(window) >= self._max_per_minute:
                remaining = max(1, int(60 - (now - window[0])))
                logger.info("rate_limit_hit_per_minute",
                            user_id=user_id, retry_after=remaining)
                return False, "rate_limited"

            window.append(now)

            if user_id not in self._locks:
                self._locks[user_id] = asyncio.Lock()

            lock = self._locks[user_id]
            if lock.locked():
                self._queues[user_id].append(message_data)
                logger.info("rate_limit_queued",
                            user_id=user_id,
                            queue_size=len(self._queues[user_id]))
                return False, "queued"

            await lock.acquire()
            return True, "process"

    async def release(self, user_id: str) -> Optional[dict]:
        """Libera el slot. Si hay mensajes encolados, devuelve el siguiente."""
        async with self._global_lock:
            lock = self._locks.get(user_id)
            if lock and lock.locked():
                lock.release()

            queue = self._queues.get(user_id)
            if queue:
                try:
                    next_msg = queue.popleft()
                    if not queue:
                        del self._queues[user_id]
                    return next_msg
                except IndexError:
                    pass
            return None


rate_limiter = RateLimiter(max_per_minute=5)