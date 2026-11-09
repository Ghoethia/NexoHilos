"""Patrón Observer: los nodos y el mediador publican hechos sin conocer a quien los escucha."""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field


@dataclass
class Event:
    name: str
    data: dict = field(default_factory=dict)


class Observer(ABC):
    @abstractmethod
    def update(self, event: Event) -> None:
        """Recibe un hecho ya ocurrido. No debe bloquear el despacho de tareas."""


class EventBus:
    """Sujeto del patrón Observer. Entrega cada evento a una copia de la lista de suscriptores."""

    def __init__(self) -> None:
        self._subs: list[Observer] = []
        self._lock = threading.Lock()
        self._log = logging.getLogger("nexohilos.observador")

    def subscribe(self, observer: Observer) -> None:
        with self._lock:
            self._subs.append(observer)

    def publish(self, event: Event) -> None:
        with self._lock:
            subscribers = list(self._subs)
        for observer in subscribers:
            try:
                observer.update(event)
            except Exception:
                # Un observador defectuoso no puede tumbar el despacho de tareas.
                self._log.exception("el observador %s falló al recibir %s", type(observer).__name__, event.name)


class LogObserver(Observer):
    def __init__(self) -> None:
        self._log = logging.getLogger("nexohilos.observador")

    def update(self, event: Event) -> None:
        self._log.info("evento %s %s", event.name, event.data)


class MetricsObserver(Observer):
    """Acumula conteos para la evaluación. Es otro observador: el mediador no lo conoce por nombre."""

    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._lock = threading.Lock()

    def update(self, event: Event) -> None:
        with self._lock:
            self._counts[event.name] += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)
