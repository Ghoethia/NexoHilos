"""Proceso coordinador. Aloja al mediador, al bus de eventos y al registro de nodos."""

from __future__ import annotations

import argparse
import logging
import socket
import threading

from nexohilos.logsetup import configurar_log
from nexohilos.mediator import TaskMediator, crear_estrategia
from nexohilos.observer import EventBus, LogObserver, MetricsObserver
from nexohilos.protocol import recv_message, send_message

log = logging.getLogger("nexohilos.coordinador")


class CoordinatorService:
    def __init__(self, host: str, port: int, strategy_name: str, timeout: float, stale_after: float) -> None:
        self.host = host
        self.port = port
        self.strategy_name = strategy_name
        self.timeout = timeout
        self.stale_after = stale_after
        self.bus = EventBus()
        self.metrics = MetricsObserver()
        self.bus.subscribe(LogObserver())
        self.bus.subscribe(self.metrics)
        self.mediator = TaskMediator(
            self.bus,
            crear_estrategia(strategy_name),
            timeout=timeout,
        )
        self._stop = threading.Event()

    def serve(self) -> None:
        watcher = threading.Thread(target=self._watch_heartbeats, name="vigilancia-latidos", daemon=True)
        watcher.start()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(128)
        server.settimeout(0.5)
        log.info(
            "coordinador escuchando en %s:%s estrategia=%s",
            self.host,
            self.port,
            self.strategy_name,
        )
        try:
            while not self._stop.is_set():
                try:
                    conn, _addr = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
        finally:
            server.close()

    def _watch_heartbeats(self) -> None:
        while not self._stop.is_set():
            fallen = self.mediator.expire_stale(self.stale_after)
            for node_id in fallen:
                log.warning("nodo %s marcado caído por latido vencido", node_id)
            self._stop.wait(0.3)

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(10)
            try:
                message = recv_message(conn)
            except (OSError, ValueError, ConnectionError, TimeoutError):
                return
            kind = message.get("type")
            conn.settimeout(None)
            try:
                if kind == "HEARTBEAT":
                    self.mediator.heartbeat(message)
                    send_message(conn, {"type": "ACK"})
                elif kind == "SUBMIT":
                    send_message(conn, self.mediator.dispatch(message))
                elif kind == "SUBMIT_BATCH":
                    send_message(
                        conn,
                        {
                            "type": "BATCH_RESULT",
                            "results": self.mediator.dispatch_ordered(list(message.get("tasks", []))),
                        },
                    )
                elif kind == "STATUS":
                    send_message(
                        conn,
                        self.mediator.status(self.metrics.snapshot(), self.strategy_name),
                    )
                elif kind == "SHUTDOWN":
                    self._stop.set()
                    send_message(conn, {"type": "ACK"})
                else:
                    send_message(conn, {"type": "ERROR", "error": f"tipo desconocido: {kind}"})
            except OSError:
                return


def main() -> None:
    configurar_log()
    parser = argparse.ArgumentParser(description="Coordinador de NexoHilos")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--strategy", choices=("least_loaded", "round_robin"), default="least_loaded")
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--stale-after", type=float, default=1.6)
    args = parser.parse_args()
    CoordinatorService(args.host, args.port, args.strategy, args.timeout, args.stale_after).serve()


if __name__ == "__main__":
    main()
