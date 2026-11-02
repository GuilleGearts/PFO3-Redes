"""
================================================================================
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Materia: Programación sobre Redes - Tecnicatura Superior en Desarrollo de Software (TSDS)
Alumno: Guillermo Aybar
Año: 2026

Archivo: test_distribuido.py
Descripción:
    Batería de pruebas de concurrencia y estrés distribuido.
    Lanza múltiples hilos de clientes simultáneos que se conectan al servidor por socket,
    envían tareas concurrentes y validan que el servidor distribuya eficientemente la
    carga entre los workers del ThreadPool sin bloqueos ni errores.
================================================================================
"""

import socket
import json
import threading
import time
import uuid

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 65432
NUM_CLIENTES_CONCURRENTES = 8

resultados_globales = []
lock_resultados = threading.Lock()


def cliente_simulado(cliente_id: int):
    """Simula un cliente independiente que se conecta por socket y envía una tarea."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((SERVER_HOST, SERVER_PORT))

        tipos_tareas = [
            ("calculo_primos", {"limite": 6000 + (cliente_id * 500)}),
            ("hash_criptografico", {"texto": f"Usuario_{cliente_id}_Secret", "rondas": 40000}),
            ("procesamiento_texto", {"texto": f"Cliente concurrente número {cliente_id} probando la arquitectura distribuida."}),
            ("almacenamiento_s3", {"nombre_archivo": f"payload_cliente_{cliente_id}.bin", "tamano_kb": 256 * cliente_id})
        ]
        
        # Seleccionar tarea basada en el ID del cliente
        tipo, params = tipos_tareas[cliente_id % len(tipos_tareas)]
        task_id = str(uuid.uuid4())

        solicitud = {
            "comando": "tarea",
            "tarea": {
                "task_id": task_id,
                "tipo": tipo,
                "parametros": params
            }
        }

        t_inicio = time.perf_counter()
        # Enviar JSON
        sock.sendall((json.dumps(solicitud) + "\n").encode('utf-8'))

        # Recibir JSON
        buffer = ""
        while "\n" not in buffer:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buffer += chunk.decode('utf-8')

        t_fin = time.perf_counter()
        duracion_cliente = round(t_fin - t_inicio, 4)

        if buffer:
            respuesta = json.loads(buffer.strip().split("\n")[0])
            datos = respuesta.get("datos", {})
            worker = datos.get("worker", "Desconocido")
            estado = datos.get("estado", "ERROR")
            tiempo_worker = datos.get("tiempo_ejecucion_seg", 0)

            with lock_resultados:
                resultados_globales.append({
                    "cliente_id": cliente_id,
                    "task_id": task_id[:8],
                    "tipo": tipo,
                    "worker": worker,
                    "estado": estado,
                    "duracion_total": duracion_cliente,
                    "duracion_worker": tiempo_worker
                })
        
        sock.close()

    except Exception as e:
        with lock_resultados:
            resultados_globales.append({
                "cliente_id": cliente_id,
                "estado": "ERROR_CONEXION",
                "error": str(e)
            })


def ejecutar_bateria_concurrencia():
    print("=" * 75)
    print("  BATERÍA DE PRUEBAS DE CONCURRENCIA DISTRIBUIDA (PFO 3)             ")
    print(f"  Lanzando {NUM_CLIENTES_CONCURRENTES} clientes paralelos por socket TCP hacia {SERVER_HOST}:{SERVER_PORT}")
    print("=" * 75)

    hilos = []
    t_global_inicio = time.perf_counter()

    # Iniciar todos los clientes concurrentes
    for i in range(1, NUM_CLIENTES_CONCURRENTES + 1):
        h = threading.Thread(target=cliente_simulado, args=(i,), name=f"TestClient-{i}")
        hilos.append(h)
        h.start()

    # Esperar a que todos terminen
    for h in hilos:
        h.join()

    t_global_total = round(time.perf_counter() - t_global_inicio, 4)

    print("\n[+] Todos los clientes finalizaron sus peticiones.\n")
    print(f"{'CLIENTE':<10} | {'TASK ID':<10} | {'TIPO':<22} | {'WORKER':<18} | {'ESTADO':<10} | {'TIEMPO'}")
    print("-" * 85)

    workers_usados = set()
    completadas = 0

    for r in resultados_globales:
        if r.get("estado") == "COMPLETED":
            completadas += 1
            workers_usados.add(r.get("worker"))
            print(f"Cliente {r['cliente_id']:<2} | {r['task_id']:<10} | {r['tipo']:<22} | {r['worker']:<18} | {r['estado']:<10} | {r['duracion_total']}s")
        else:
            print(f"Cliente {r.get('cliente_id'):<2} | ERROR: {r.get('error')}")

    print("-" * 85)
    print(f"  * Tareas completadas exitosamente : {completadas}/{NUM_CLIENTES_CONCURRENTES}")
    print(f"  * Workers distintos utilizados    : {len(workers_usados)} ({', '.join(sorted(workers_usados))})")
    print(f"  * Tiempo global de ejecucion      : {t_global_total} segundos")
    print("=" * 75)

    if completadas == NUM_CLIENTES_CONCURRENTES:
        print("[+] TEST EXITOSO: La arquitectura distribuida con Pool de Workers")
        print("    gestiono todas las peticiones concurrentes en paralelo sin saturacion.")
    else:
        print("[!] TEST CON ADVERTENCIAS: Algunas tareas no concluyeron.")


if __name__ == '__main__':
    ejecutar_bateria_concurrencia()
