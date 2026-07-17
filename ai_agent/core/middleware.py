import time
from collections import defaultdict, deque

from fastapi import Request
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting para endpoints REST públicos (/login, /chat).

    /login: 5 req/min por IP (anti brute-force)
    /chat:  10 req/min por token de usuario
    """

    def __init__(self, app):
        super().__init__(app)
        self._login: dict[str, deque[float]] = defaultdict(lambda: deque())
        self._chat: dict[str, deque[float]] = defaultdict(lambda: deque())

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        now = time.monotonic()

        if path == "/login":
            key = request.client.host if request.client else "unknown"
            window = self._login[key]
            while window and now - window[0] > 60:
                window.popleft()
            if len(window) >= 5:
                logger.info("rest_rate_limit_hit", path=path, ip=key, type="login")
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Demasiados intentos. Espera un momento."},
                )
            window.append(now)

        elif path == "/chat":
            auth = request.headers.get("authorization", "")
            key = auth.replace("Bearer ", "")[:20] if auth.startswith("Bearer ") else (
                request.client.host if request.client else "unknown"
            )
            window = self._chat[key]
            while window and now - window[0] > 60:
                window.popleft()
            if len(window) >= 10:
                logger.info("rest_rate_limit_hit", path=path, user=key, type="chat")
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Demasiadas solicitudes. Espera un momento."},
                )
            window.append(now)

        return await call_next(request)