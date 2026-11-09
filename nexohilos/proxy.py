"""Patrón Proxy remoto: el mediador pide un resultado y no abre sockets por su cuenta."""

from __future__ import annotations

import json
import logging
import socket

from nexohilos.protocol import recv_message, send_message

log = logging.getLogger("nexohilos.proxy")


class ProxyError(Exception):
    """Fallo de comunicación con el nodo. El mediador decide si reasigna la tarea."""


# Un corte ya establecido no se reintenta en el mismo nodo: el proceso se fue.
_SIN_REINTENTO = (ConnectionResetError, ConnectionAbortedError, ConnectionRefusedError, BrokenPipeError)


class NodeProxy:
    """Sustituto de un nodo remoto.

    Separa dos esperas. La conexión usa un límite corto: en el equipo de prueba un puerto
    cerrado no responde con rechazo, así que sin ese límite el cliente se quedaría colgado.
    La lectura usa un límite largo, porque la tarea puede estar en el pool del nodo.

    Si el nodo ejecutó la tarea y la respuesta se pierde, un reintento vuelve a ejecutarla
    (semántica de al menos una vez). Aquí las tareas son puras: el resumen depende solo de
    la entrada y repetirlas no corrompe estado.
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 8.0,
        connect_timeout: float = 0.4,
        retries: int = 1,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.retries = retries

    def invoke(self, message: dict) -> dict:
        last_error: Exception | None = None
        attempts = self.retries + 1
        for attempt in range(1, attempts + 1):
            try:
                return self._once(message)
            except _SIN_REINTENTO as exc:
                log.warning("proxy %s:%s cortó la conexión: %s", self.host, self.port, exc)
                raise ProxyError(f"{self.host}:{self.port} conexión cortada ({exc})") from exc
            except (OSError, TimeoutError, ConnectionError, json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                log.warning(
                    "proxy %s:%s intento %s/%s falló: %s",
                    self.host,
                    self.port,
                    attempt,
                    attempts,
                    exc,
                )
        raise ProxyError(f"{self.host}:{self.port} sin respuesta ({last_error})")

    def _once(self, message: dict) -> dict:
        with socket.create_connection((self.host, self.port), timeout=self.connect_timeout) as sock:
            sock.settimeout(self.timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            send_message(sock, message)
            return recv_message(sock)
