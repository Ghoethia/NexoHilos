# NexoHilos

Prototipo de una arquitectura distribuida de procesamiento de tareas. Cada nodo es un proceso con su puerto TCP y un pool de hilos. Un coordinador reparte el trabajo. Los nodos no se conocen entre sí.

Curso: Arquitectura de Software, Unidad 2. Actividad: Tejiendo redes.
Estudiante: Jhoan Alvarez (jhoan.alvarez214@pascualbravo.co). Trabajo individual.

## Qué demuestra

- Comunicación real entre procesos por TCP y mensajes JSON.
- Hilos dentro del nodo para tareas de espera, y el límite del cerrojo global cuando la tarea es CPU.
- Caída de un nodo y reasignación de la tarea.
- Patrones Proxy, Mediator, Observer y Strategy, más un pool de hilos.

## Ejecutar

Desde esta carpeta:

```powershell
python -m unittest discover -s tests -v
python scripts\demo.py
python scripts\evaluar.py
python scripts\generar_diagramas.py
python scripts\generar_informe.py
python scripts\generar_word.py
python scripts\generar_presentacion.py
```

La demo levanta tres nodos, muestra el hilo que atendió cada tarea, mata el nodo `n2` y sigue. La evaluación repite diez escenarios tres veces y escribe `docs/resultados_evaluacion.json`. El informe y las diapositivas leen esas cifras: no hay que copiarlas a mano.

El prototipo solo usa la biblioteca estándar. Para el PDF, las figuras y las diapositivas:

```powershell
python -m pip install -r requirements.txt
```

## Mapa del código

| Módulo | Rol |
| --- | --- |
| `nexohilos/coordinator.py` | Proceso coordinador |
| `nexohilos/worker_node.py` | Proceso nodo, latido y pool |
| `nexohilos/mediator.py` | Mediator y estrategias de reparto |
| `nexohilos/proxy.py` | Proxy remoto, tiempos de espera y reintento |
| `nexohilos/observer.py` | Bus, bitácora y métricas |
| `nexohilos/protocol.py` | JSON delimitado por salto de línea |
| `nexohilos/client.py` | Cliente que mide la latencia |
| `docs/uml/` | Fuente PlantUML de los diagramas |

Arranque manual:

```powershell
python -m nexohilos.coordinator --port 9100 --strategy least_loaded
python -m nexohilos.worker_node --id n1 --port 9210 --coordinator-port 9100 --threads 4 --mode io
```

## Entrega

- Informe para editar: `docs/NexoHilos_informe.docx`
- Informe en PDF: `docs/NexoHilos_informe.pdf`
- Diapositivas: `docs/NexoHilos_presentacion.pptx`
- Guion del video (12 a 15 minutos): `docs/guion_video.md`
- El video lo graba el estudiante y lo sube a YouTube como no listado. El enlace se pega en el LMS.

## Publicar el repositorio

GitHub no acepta la contraseña de la cuenta para `git push`. Hace falta un token personal o SSH, creados desde la cuenta institucional. No pegar esa clave en el chat ni en el código.
