"""Punto de entrada informativo: python -m nexohilos"""

print(
    "NexoHilos 1.0.0\n"
    "  python -m nexohilos.coordinator --port 9100 --strategy least_loaded\n"
    "  python -m nexohilos.worker_node --id n1 --port 9210 "
    "--coordinator-port 9100 --threads 4\n"
    "  python scripts/demo.py\n"
    "  python scripts/evaluar.py"
)
