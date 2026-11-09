"""Cliente del coordinador. Mide la latencia de extremo a extremo de cada tarea."""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from nexohilos.protocol import recv_message, send_message


def rpc(host: str, port: int, message: dict, timeout: float = 40.0) -> dict:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        send_message(sock, message)
        return recv_message(sock)


def submit_task(host: str, port: int, task: dict, timeout: float = 40.0) -> dict:
    inicio = time.perf_counter()
    try:
        respuesta = rpc(host, port, {"type": "SUBMIT", **task}, timeout=timeout)
    except OSError as exc:
        return {
            "type": "RESULT",
            "task_id": task.get("task_id"),
            "status": "error",
            "error": str(exc),
            "attempts": 0,
            "e2e_ms": round((time.perf_counter() - inicio) * 1000, 2),
        }
    respuesta["e2e_ms"] = round((time.perf_counter() - inicio) * 1000, 2)
    return respuesta


def submit_in_order(host: str, port: int, tasks: list[dict], timeout: float = 40.0) -> list[dict]:
    """Envía el lote en el orden de la lista y deja que los nodos trabajen en paralelo.

    Hace falta para comparar estrategias: si cada tarea esperara a la anterior,
    la carga volvería a cero y el turno se vería igual que la menor carga.
    """
    abiertos: list[tuple[socket.socket, float, dict]] = []
    try:
        for task in tasks:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.settimeout(timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            send_message(sock, {"type": "SUBMIT", **task})
            abiertos.append((sock, time.perf_counter(), task))
        resultados = []
        for sock, enviado, task in abiertos:
            try:
                respuesta = recv_message(sock)
            except (OSError, ValueError, ConnectionError) as exc:
                respuesta = {
                    "type": "RESULT",
                    "task_id": task.get("task_id"),
                    "status": "error",
                    "error": str(exc),
                    "attempts": 0,
                }
            respuesta["e2e_ms"] = round((time.perf_counter() - enviado) * 1000, 2)
            resultados.append(respuesta)
        return resultados
    finally:
        for sock, _enviado, _task in abiertos:
            sock.close()


def submit_batch(host: str, port: int, tasks: list[dict], timeout: float = 60.0) -> list[dict]:
    """Un solo pedido. El coordinador reserva los nodos en orden y luego ejecuta en paralelo."""
    respuesta = rpc(host, port, {"type": "SUBMIT_BATCH", "tasks": tasks}, timeout=timeout)
    resultados = list(respuesta.get("results", []))
    if len(resultados) != len(tasks):
        return [
            {"task_id": task.get("task_id"), "status": "error", "error": "respuesta de lote incompleta"}
            for task in tasks
        ]
    return resultados


def submit_many(host: str, port: int, tasks: list[dict], concurrency: int) -> list[dict]:
    results: list[dict | None] = [None] * len(tasks)
    lock = threading.Lock()

    def work(index: int, task: dict) -> None:
        result = submit_task(host, port, task)
        with lock:
            results[index] = result

    workers = max(1, min(concurrency, len(tasks) or 1))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(work, index, task) for index, task in enumerate(tasks)]
        for future in futures:
            future.result()
    return [result if result is not None else {"status": "error", "error": "sin resultado"} for result in results]
