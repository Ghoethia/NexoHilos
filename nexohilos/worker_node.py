"""Nodo trabajador: un proceso con socket propio y un pool acotado de hilos."""

from __future__ import annotations

import argparse
import hashlib
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from nexohilos.logsetup import configurar_log
from nexohilos.protocol import recv_message, send_message

log = logging.getLogger("nexohilos.nodo")

# Iteraciones por unidad de complejidad. El valor cabe en el tiempo de espera del proxy
# y basta para que el costo de CPU sea visible frente a la E/S simulada.
ITERACIONES_CPU = 20000


def ejecutar_tarea(task: dict, node_id: str, mode: str) -> dict:
    inicio = time.perf_counter()
    complejidad = max(1, int(task.get("complexity", 1)))
    if mode == "cpu":
        acc = b"nexohilos"
        for _ in range(ITERACIONES_CPU * complejidad):
            acc = hashlib.sha256(acc).digest()
        digest = acc.hex()
    else:
        # time.sleep libera el GIL: los hilos del mismo proceso sí avanzan en paralelo.
        time.sleep(0.08 * complejidad)
        digest = hashlib.sha256(f"{node_id}:{task.get('task_id')}:{complejidad}".encode()).hexdigest()
    return {
        "type": "RESULT",
        "task_id": task.get("task_id"),
        "status": "ok",
        "elapsed_ms": round((time.perf_counter() - inicio) * 1000, 2),
        "digest": digest[:16],
        "worker": threading.current_thread().name,
        "node_id": node_id,
        "mode": mode,
        "complexity": complejidad,
    }


class WorkerNode:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        coordinator_host: str,
        coordinator_port: int,
        threads: int,
        mode: str,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.threads = threads
        self.mode = mode
        self.pool = ThreadPoolExecutor(max_workers=threads, thread_name_prefix=f"{node_id}-hilo")
        self.inflight = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def serve(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(64)
        server.settimeout(0.5)
        log.info(
            "nodo %s listo en %s:%s con %s hilos (modo %s)",
            self.node_id,
            self.host,
            self.port,
            self.threads,
            self.mode,
        )
        threading.Thread(target=self._heartbeat_loop, name=f"{self.node_id}-latido", daemon=True).start()
        try:
            while not self._stop.is_set():
                try:
                    conn, _addr = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
        finally:
            server.close()
            self.pool.shutdown(wait=False, cancel_futures=True)

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                with self._lock:
                    inflight = self.inflight
                payload = {
                    "type": "HEARTBEAT",
                    "node_id": self.node_id,
                    "host": self.host,
                    "port": self.port,
                    "capacity": self.threads,
                    "inflight": inflight,
                    "mode": self.mode,
                }
                with socket.create_connection(
                    (self.coordinator_host, self.coordinator_port), timeout=1.0
                ) as sock:
                    sock.settimeout(1.0)
                    send_message(sock, payload)
                    recv_message(sock)
            except OSError:
                pass
            self._stop.wait(0.4)

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(30)
            try:
                message = recv_message(conn)
            except (OSError, ValueError, ConnectionError, TimeoutError):
                return
            if message.get("type") == "SHUTDOWN":
                self._stop.set()
                send_message(conn, {"type": "ACK"})
                return
            if message.get("type") != "TASK":
                send_message(conn, {"type": "ERROR", "error": "tipo no soportado"})
                return
            with self._lock:
                self.inflight += 1
            log.info("nodo %s recibe %s en un hilo del pool", self.node_id, message.get("task_id"))
            try:
                result = self.pool.submit(ejecutar_tarea, message, self.node_id, self.mode).result()
                log.info(
                    "nodo %s terminó %s en %s (%s ms)",
                    self.node_id,
                    message.get("task_id"),
                    result.get("worker"),
                    result.get("elapsed_ms"),
                )
                send_message(conn, result)
            except Exception as exc:
                send_message(
                    conn,
                    {
                        "type": "RESULT",
                        "task_id": message.get("task_id"),
                        "status": "error",
                        "error": str(exc),
                        "node_id": self.node_id,
                    },
                )
            finally:
                with self._lock:
                    self.inflight -= 1


def main() -> None:
    configurar_log()
    parser = argparse.ArgumentParser(description="Nodo trabajador de NexoHilos")
    parser.add_argument("--id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--coordinator-host", default="127.0.0.1")
    parser.add_argument("--coordinator-port", type=int, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--mode", choices=("io", "cpu"), default="io")
    args = parser.parse_args()
    WorkerNode(
        node_id=args.id,
        host=args.host,
        port=args.port,
        coordinator_host=args.coordinator_host,
        coordinator_port=args.coordinator_port,
        threads=args.threads,
        mode=args.mode,
    ).serve()


if __name__ == "__main__":
    main()
