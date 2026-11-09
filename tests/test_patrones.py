"""Pruebas de los patrones y de la reasignación cuando un nodo no responde."""

from __future__ import annotations

import socket
import sys
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nexohilos.mediator import (  # noqa: E402
    LeastLoadedStrategy,
    RoundRobinStrategy,
    TaskMediator,
)
from nexohilos.observer import Event, EventBus, MetricsObserver, Observer  # noqa: E402
from nexohilos.protocol import encode_message, recv_message, send_message  # noqa: E402
from nexohilos.proxy import NodeProxy  # noqa: E402


class _Spy(Observer):
    def __init__(self) -> None:
        self.events = []

    def update(self, event: Event) -> None:
        self.events.append(event)


class ObserverTests(unittest.TestCase):
    def test_todos_los_suscriptores_reciben_el_evento(self) -> None:
        bus = EventBus()
        first, second = _Spy(), _Spy()
        metrics = MetricsObserver()
        bus.subscribe(first)
        bus.subscribe(second)
        bus.subscribe(metrics)
        bus.publish(Event("NODE_UP", {"node_id": "n1"}))
        self.assertEqual(first.events[0].name, "NODE_UP")
        self.assertEqual(second.events[0].data["node_id"], "n1")
        self.assertEqual(metrics.snapshot()["NODE_UP"], 1)

    def test_un_observador_roto_no_bloquea_al_resto(self) -> None:
        class Roto(Observer):
            def update(self, event: Event) -> None:
                raise RuntimeError("fallo de prueba")

        bus = EventBus()
        spy = _Spy()
        bus.subscribe(Roto())
        bus.subscribe(spy)
        bus.publish(Event("TASK_COMPLETED", {}))
        self.assertEqual(len(spy.events), 1)


class StrategyTests(unittest.TestCase):
    def _mediator(self, strategy) -> TaskMediator:
        mediator = TaskMediator(EventBus(), strategy, timeout=1.0)
        for node_id, port in (("n1", 1), ("n2", 2)):
            mediator.heartbeat(
                {
                    "node_id": node_id,
                    "host": "127.0.0.1",
                    "port": port,
                    "capacity": 2,
                    "mode": "io",
                }
            )
        return mediator

    def test_menor_carga_prefiere_el_nodo_mas_libre(self) -> None:
        mediator = self._mediator(LeastLoadedStrategy())
        mediator.nodes["n1"].load = 9
        mediator.nodes["n2"].load = 2
        self.assertEqual(mediator.nodo_elegido(), "n2")

    def test_empate_de_carga_rota_entre_nodos(self) -> None:
        mediator = self._mediator(LeastLoadedStrategy())
        chosen = [mediator.nodo_elegido() for _ in range(4)]
        self.assertEqual(chosen, ["n1", "n2", "n1", "n2"])

    def test_turno_alterna_sin_mirar_la_carga(self) -> None:
        mediator = self._mediator(RoundRobinStrategy())
        mediator.nodes["n1"].load = 0
        mediator.nodes["n2"].load = 100
        chosen = [mediator.nodo_elegido() for _ in range(4)]
        self.assertEqual(chosen, ["n1", "n2", "n1", "n2"])

    def test_latido_vencido_publica_node_down(self) -> None:
        bus = EventBus()
        spy = _Spy()
        bus.subscribe(spy)
        mediator = TaskMediator(bus, LeastLoadedStrategy())
        mediator.heartbeat(
            {"node_id": "n1", "host": "127.0.0.1", "port": 9, "capacity": 1}
        )
        mediator.nodes["n1"].last_seen = time.time() - 10
        fallen = mediator.expire_stale(1.5)
        self.assertEqual(fallen, ["n1"])
        self.assertFalse(mediator.nodes["n1"].healthy)
        self.assertEqual(spy.events[-1].name, "NODE_DOWN")
        # Un segundo ciclo no debe repetir el evento: la transición ya ocurrió.
        self.assertEqual(mediator.expire_stale(1.5), [])


class ProtocolTests(unittest.TestCase):
    def test_mensaje_termina_en_salto_de_linea(self) -> None:
        raw = encode_message({"type": "ACK", "nota": "á"})
        self.assertTrue(raw.endswith(b"\n"))
        self.assertIn("á".encode("utf-8"), raw)


class NetworkTests(unittest.TestCase):
    def test_proxy_entrega_la_respuesta_del_nodo(self) -> None:
        ready = threading.Event()
        holder: dict[str, int] = {}

        def serve() -> None:
            server = socket.socket()
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("127.0.0.1", 0))
            holder["port"] = server.getsockname()[1]
            server.listen(1)
            ready.set()
            conn, _addr = server.accept()
            with conn:
                incoming = recv_message(conn)
                send_message(conn, {"type": "RESULT", "task_id": incoming["task_id"], "status": "ok"})
            server.close()

        threading.Thread(target=serve, daemon=True).start()
        self.assertTrue(ready.wait(3))
        result = NodeProxy("127.0.0.1", holder["port"], timeout=2, retries=0).invoke(
            {"type": "TASK", "task_id": "t1"}
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["task_id"], "t1")

    def test_el_mediador_reasigna_si_el_primer_nodo_no_existe(self) -> None:
        ready = threading.Event()
        holder: dict[str, int] = {}

        def serve() -> None:
            server = socket.socket()
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("127.0.0.1", 0))
            holder["port"] = server.getsockname()[1]
            server.listen(1)
            ready.set()
            conn, _addr = server.accept()
            with conn:
                incoming = recv_message(conn)
                send_message(
                    conn,
                    {
                        "type": "RESULT",
                        "task_id": incoming["task_id"],
                        "status": "ok",
                        "elapsed_ms": 1,
                        "worker": "hilo-prueba",
                        "node_id": "b-bueno",
                    },
                )
            server.close()

        threading.Thread(target=serve, daemon=True).start()
        self.assertTrue(ready.wait(3))
        dead = socket.socket()
        dead.bind(("127.0.0.1", 0))
        dead_port = dead.getsockname()[1]
        dead.close()

        bus = EventBus()
        spy = _Spy()
        bus.subscribe(spy)
        mediator = TaskMediator(bus, LeastLoadedStrategy(), timeout=1.0, max_attempts=3)
        mediator.heartbeat(
            {"node_id": "a-caido", "host": "127.0.0.1", "port": dead_port, "capacity": 1}
        )
        mediator.heartbeat(
            {"node_id": "b-bueno", "host": "127.0.0.1", "port": holder["port"], "capacity": 1}
        )
        result = mediator.dispatch({"task_id": "t1", "complexity": 1, "payload": "x"})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["node_id"], "b-bueno")
        self.assertGreaterEqual(result["attempts"], 2)
        names = [event.name for event in spy.events]
        self.assertIn("TASK_REASSIGNED", names)
        self.assertIn("TASK_COMPLETED", names)
        self.assertFalse(mediator.nodes["a-caido"].healthy)


if __name__ == "__main__":
    unittest.main()
