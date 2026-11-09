"""Mediator y Strategy: los nodos no se conocen; la política de asignación se puede cambiar."""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from nexohilos.observer import Event, EventBus
from nexohilos.proxy import NodeProxy, ProxyError


@dataclass
class NodeState:
    node_id: str
    host: str
    port: int
    capacity: int
    mode: str = "io"
    assigned: int = 0
    load: float = 0.0
    inflight: int = 0
    healthy: bool = True
    last_seen: float = 0.0
    down_reason: str = ""


class AssignmentStrategy(ABC):
    """Patrón Strategy. El mediador pide un nodo y no conoce la fórmula de la decisión."""

    @abstractmethod
    def choose(self, nodes: list[NodeState]) -> NodeState | None:
        pass


class LeastLoadedStrategy(AssignmentStrategy):
    """Elige el nodo sano con menos carga acumulada.

    Si varios nodos empatan, rota entre ellos. Sin esa rotación, un cliente que
    envía una tarea y espera el resultado vuelve siempre al mismo nodo: al
    terminar, la carga queda en cero y el desempate por nombre repite el primero.
    """

    def __init__(self) -> None:
        self._i = 0

    def choose(self, nodes: list[NodeState]) -> NodeState | None:
        if not nodes:
            return None
        minimo = min(node.load for node in nodes)
        empatados = sorted(
            (node for node in nodes if node.load == minimo),
            key=lambda node: node.node_id,
        )
        elegido = empatados[self._i % len(empatados)]
        self._i += 1
        return elegido


class RoundRobinStrategy(AssignmentStrategy):
    """Reparte por turno, sin mirar si la tarea es corta o larga. Sirve de línea base."""

    def __init__(self) -> None:
        self._i = 0

    def choose(self, nodes: list[NodeState]) -> NodeState | None:
        if not nodes:
            return None
        ordered = sorted(nodes, key=lambda node: node.node_id)
        node = ordered[self._i % len(ordered)]
        self._i += 1
        return node


def crear_estrategia(nombre: str) -> AssignmentStrategy:
    if nombre == "round_robin":
        return RoundRobinStrategy()
    if nombre == "least_loaded":
        return LeastLoadedStrategy()
    raise ValueError(f"estrategia desconocida: {nombre}")


class TaskMediator:
    """Los clientes y los nodos hablan con este mediador. Ningún nodo conoce a otro nodo."""

    def __init__(
        self,
        bus: EventBus,
        strategy: AssignmentStrategy,
        timeout: float = 8.0,
        connect_timeout: float = 0.4,
        max_attempts: int = 3,
    ) -> None:
        self.bus = bus
        self.strategy = strategy
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.max_attempts = max_attempts
        self.nodes: dict[str, NodeState] = {}
        self._lock = threading.Lock()

    def heartbeat(self, message: dict) -> None:
        node_id = str(message["node_id"])
        with self._lock:
            node = self.nodes.get(node_id)
            is_new = node is None
            recovered = node is not None and not node.healthy
            if is_new:
                node = NodeState(
                    node_id=node_id,
                    host=message["host"],
                    port=int(message["port"]),
                    capacity=int(message["capacity"]),
                )
                self.nodes[node_id] = node
            node.host = message["host"]
            node.port = int(message["port"])
            node.capacity = int(message["capacity"])
            node.inflight = int(message.get("inflight", 0))
            node.mode = str(message.get("mode", "io"))
            node.last_seen = time.time()
            node.healthy = True
            node.down_reason = ""
        if is_new or recovered:
            self.bus.publish(Event("NODE_UP", {"node_id": node_id, "recovered": recovered}))

    def expire_stale(self, max_age: float) -> list[str]:
        now = time.time()
        fallen: list[str] = []
        with self._lock:
            for node in self.nodes.values():
                if node.healthy and (now - node.last_seen) > max_age:
                    node.healthy = False
                    node.down_reason = "latido vencido"
                    fallen.append(node.node_id)
        for node_id in fallen:
            self.bus.publish(Event("NODE_DOWN", {"node_id": node_id, "reason": "latido vencido"}))
        return fallen

    def nodo_elegido(self, excluded: set[str] | None = None) -> str | None:
        with self._lock:
            node = self._select_locked(excluded or set())
            return None if node is None else node.node_id

    def dispatch(self, task: dict) -> dict:
        return self._despachar(task)

    def dispatch_ordered(self, tasks: list[dict]) -> list[dict]:
        """Reserva los nodos en el orden de la lista y solo después ejecuta en paralelo.

        Así la comparación entre turno y menor carga depende de la política, no de qué
        hilo cliente llegó primero al coordinador.
        """
        plan = []
        for task in tasks:
            complexity = max(1, int(task.get("complexity", 1)))
            plan.append(self._reservar(set(), complexity))
        results: list[dict | None] = [None] * len(tasks)
        workers = [
            threading.Thread(target=self._correr_reservada, args=(index, task, reserved, results))
            for index, (task, reserved) in enumerate(zip(tasks, plan))
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        return [item if item is not None else {"status": "error", "error": "sin resultado"} for item in results]

    def _correr_reservada(self, index: int, task: dict, reserved, results: list) -> None:
        results[index] = self._despachar(task, reservation=reserved)

    def _despachar(self, task: dict, reservation=None) -> dict:
        task_id = str(task.get("task_id", ""))
        complexity = max(1, int(task.get("complexity", 1)))
        excluded: set[str] = set()
        attempts = 0
        last_error = "sin nodos disponibles"
        inicio = time.perf_counter()
        while attempts < self.max_attempts:
            if reservation is None:
                reservation = self._reservar(excluded, complexity)
            if reservation is None:
                break
            node_id, host, port, reserved_complexity = reservation
            reservation = None
            attempts += 1
            self.bus.publish(
                Event(
                    "TASK_ASSIGNED",
                    {"task_id": task_id, "node_id": node_id, "attempt": attempts, "complexity": complexity},
                )
            )
            message = {
                "type": "TASK",
                "task_id": task_id,
                "complexity": complexity,
                "payload": task.get("payload", ""),
            }
            try:
                result = NodeProxy(
                    host,
                    port,
                    timeout=self.timeout,
                    connect_timeout=self.connect_timeout,
                    retries=1,
                ).invoke(message)
            except ProxyError as exc:
                self._soltar_y_marcar(node_id, reserved_complexity, str(exc))
                excluded.add(node_id)
                last_error = str(exc)
                self.bus.publish(Event("TASK_REASSIGNED", {"task_id": task_id, "from": node_id, "attempt": attempts}))
                continue
            except Exception as exc:
                with self._lock:
                    self._release_locked(node_id, reserved_complexity)
                excluded.add(node_id)
                last_error = str(exc)
                continue
            with self._lock:
                self._release_locked(node_id, reserved_complexity)
            if result.get("status") != "ok":
                last_error = str(result.get("error", "error de nodo"))
                excluded.add(node_id)
                self.bus.publish(Event("TASK_REASSIGNED", {"task_id": task_id, "from": node_id, "attempt": attempts}))
                continue
            result["attempts"] = attempts
            result["node_id"] = result.get("node_id") or node_id
            result["e2e_ms"] = round((time.perf_counter() - inicio) * 1000, 2)
            self.bus.publish(
                Event(
                    "TASK_COMPLETED",
                    {
                        "task_id": task_id,
                        "node_id": result["node_id"],
                        "service_ms": result.get("elapsed_ms"),
                        "attempts": attempts,
                        "worker": result.get("worker"),
                    },
                )
            )
            return result
        self.bus.publish(Event("TASK_FAILED", {"task_id": task_id, "error": last_error, "attempts": attempts}))
        return {
            "type": "RESULT",
            "task_id": task_id,
            "status": "error",
            "error": last_error,
            "attempts": attempts,
            "e2e_ms": round((time.perf_counter() - inicio) * 1000, 2),
        }

    def _reservar(self, excluded: set[str], complexity: int):
        with self._lock:
            node = self._select_locked(excluded)
            if node is None:
                return None
            node.assigned += 1
            node.load += float(complexity)
            return (node.node_id, node.host, node.port, complexity)

    def status(self, events: dict[str, int], strategy_name: str) -> dict:
        now = time.time()
        with self._lock:
            nodes = [
                {
                    "node_id": node.node_id,
                    "host": node.host,
                    "port": node.port,
                    "capacity": node.capacity,
                    "assigned": node.assigned,
                    "load": round(node.load, 2),
                    "inflight": node.inflight,
                    "healthy": node.healthy,
                    "mode": node.mode,
                    "down_reason": node.down_reason,
                    "last_seen_age_ms": round((now - node.last_seen) * 1000, 1) if node.last_seen else None,
                }
                for node in sorted(self.nodes.values(), key=lambda item: item.node_id)
            ]
        return {"type": "STATUS", "strategy": strategy_name, "nodes": nodes, "events": events}

    def _select_locked(self, excluded: set[str]) -> NodeState | None:
        healthy = [node for node in self.nodes.values() if node.healthy and node.node_id not in excluded]
        return self.strategy.choose(healthy)

    def _release_locked(self, node_id: str, complexity: int) -> None:
        node = self.nodes.get(node_id)
        if node is None:
            return
        node.assigned = max(0, node.assigned - 1)
        node.load = max(0.0, node.load - complexity)

    def _mark_down_locked(self, node_id: str, reason: str) -> bool:
        node = self.nodes.get(node_id)
        if node is None or not node.healthy:
            return False
        node.healthy = False
        node.down_reason = reason
        return True

    def _soltar_y_marcar(self, node_id: str, complexity: int, reason: str) -> None:
        with self._lock:
            self._release_locked(node_id, complexity)
            changed = self._mark_down_locked(node_id, reason)
        if changed:
            self.bus.publish(Event("NODE_DOWN", {"node_id": node_id, "reason": reason}))
