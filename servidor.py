"""
================================================================================
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Materia: Programación sobre Redes - Tecnicatura Superior en Desarrollo de Software (TSDS)
Alumno: Guillermo Aybar
Año: 2026

Archivo: servidor.py
Descripción:
    Servidor distribuido basado en sockets TCP que implementa un modelo multihilo
    con distribución de tareas a través de un Pool de Workers (ThreadPoolExecutor).
    Gestiona múltiples conexiones de clientes simultáneas, desacopla la recepción
    de la ejecución de tareas intensivas y retorna los resultados de forma estructurada.
================================================================================
"""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import json
import time
import uuid
import hashlib
import sys
import os
from datetime import datetime

# ------------------------------------------------------------------------------
# Configuración del Servidor y Clúster de Workers
# ------------------------------------------------------------------------------
HOST = '127.0.0.1'       # Escucha local (o '0.0.0.0' para interfaces de red)
PORT = 65432             # Puerto TCP de escucha
MAX_WORKERS = 4          # Tamaño del pool de hilos de ejecución de tareas
BUFFER_SIZE = 4096       # Tamaño del buffer de lectura de sockets
BACKLOG = 10             # Cola de conexiones pendientes en socket.listen

# Métricas del Servidor en Memoria
estadisticas = {
    "servidor_iniciado": datetime.now().isoformat(),
    "clientes_conectados_historico": 0,
    "clientes_activos": 0,
    "tareas_recibidas": 0,
    "tareas_completadas": 0,
    "tareas_fallidas": 0
}
estadisticas_lock = threading.Lock()


# ------------------------------------------------------------------------------
# Funciones de Trabajo (Lógica de Negocio ejecutada por los Workers)
# ------------------------------------------------------------------------------

def es_primo(n: int) -> bool:
    """Función auxiliar para determinar si un número es primo."""
    if n < 2:
        return False
    if n in (2, 3):
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    w = 2
    while i * i <= n:
        if n % i == 0:
            return False
        i += w
        w = 6 - w
    return True


def ejecutar_calculo_primos(parametros: dict) -> dict:
    """Calcula números primos dentro de un rango especificado."""
    limite = parametros.get("limite", 1000)
    primos = [n for n in range(2, limite + 1) if es_primo(n)]
    return {
        "tipo": "calculo_primos",
        "limite_evaluado": limite,
        "cantidad_encontrada": len(primos),
        "ultimos_5_primos": primos[-5:] if len(primos) >= 5 else primos
    }


def ejecutar_hash_criptografico(parametros: dict) -> dict:
    """
    Ejecuta múltiples rondas de hash criptográfico con Salt para simular
    cargas de seguridad intensivas en CPU (Key Stretching).
    """
    texto = parametros.get("texto", "clave_por_defecto")
    rondas = parametros.get("rondas", 50000)
    salt = parametros.get("salt", "pfo3_salt_distribuido")
    
    valor_actual = (texto + salt).encode('utf-8')
    for _ in range(rondas):
        valor_actual = hashlib.sha256(valor_actual).digest()
        
    return {
        "tipo": "hash_criptografico",
        "rondas_ejecutadas": rondas,
        "algoritmo": "SHA-256 (Iterative Key Stretching)",
        "hash_final_hex": valor_actual.hex()
    }


def ejecutar_procesamiento_texto(parametros: dict) -> dict:
    """Analiza y procesa un texto recibido."""
    texto = parametros.get("texto", "")
    palabras = texto.split()
    frecuencias = {}
    for p in palabras:
        limpia = p.strip(".,;:!?¡¿()[]\"'").lower()
        if limpia:
            frecuencias[limpia] = frecuencias.get(limpia, 0) + 1
            
    palabras_ordenadas = sorted(frecuencias.items(), key=lambda x: x[1], reverse=True)
    
    return {
        "tipo": "procesamiento_texto",
        "total_caracteres": len(texto),
        "total_palabras": len(palabras),
        "palabras_unicas": len(frecuencias),
        "top_frecuentes": palabras_ordenadas[:5]
    }


def ejecutar_procesamiento_archivo_s3(parametros: dict) -> dict:
    """
    Simula el pipeline de almacenamiento distribuido:
    Recepción de archivo -> Procesamiento / Optimización -> Persistencia en PostgreSQL + S3.
    """
    nombre_archivo = parametros.get("nombre_archivo", "dataset.csv")
    tamano_kb = parametros.get("tamano_kb", 512)
    
    # Simula latencia de I/O a almacenamiento distribuido
    time.sleep(0.3)
    
    s3_bucket = "bucket-pfo3-produccion"
    s3_key = f"archivos/{uuid.uuid4().hex}_{nombre_archivo}"
    s3_uri = f"s3://{s3_bucket}/{s3_key}"
    pg_registro_id = int(time.time() * 1000) % 1000000
    
    return {
        "tipo": "almacenamiento_distribuido_s3_pg",
        "archivo": nombre_archivo,
        "tamano_kb": tamano_kb,
        "s3_uri": s3_uri,
        "postgres_record_id": pg_registro_id,
        "estado_persistencia": "SINCRONIZADO_ACID"
    }


def ejecutar_tarea_worker(tarea_info: dict) -> dict:
    """
    Función despachadora ejecutada dentro del ThreadPool de Workers.
    Mide los tiempos de ejecución, captura errores y estructura el resultado.
    """
    task_id = tarea_info.get("task_id", str(uuid.uuid4()))
    tipo_tarea = tarea_info.get("tipo", "calculo_primos")
    parametros = tarea_info.get("parametros", {})
    worker_nombre = threading.current_thread().name

    t_inicio = time.perf_counter()
    ts_inicio = datetime.now().isoformat()
    
    print(f"[WORKER {worker_nombre}] -> Iniciando tarea ID: {task_id[:8]} | Tipo: '{tipo_tarea}'")

    try:
        if tipo_tarea == "calculo_primos":
            datos_resultado = ejecutar_calculo_primos(parametros)
        elif tipo_tarea == "hash_criptografico":
            datos_resultado = ejecutar_hash_criptografico(parametros)
        elif tipo_tarea == "procesamiento_texto":
            datos_resultado = ejecutar_procesamiento_texto(parametros)
        elif tipo_tarea == "almacenamiento_s3" or tipo_tarea == "archivo":
            datos_resultado = ejecutar_procesamiento_archivo_s3(parametros)
        else:
            datos_resultado = {
                "mensaje": f"Tarea de tipo '{tipo_tarea}' completada con éxito.",
                "eco_parametros": parametros
            }
            
        estado = "COMPLETED"
        error_msg = None
        
        with estadisticas_lock:
            estadisticas["tareas_completadas"] += 1

    except Exception as e:
        estado = "FAILED"
        datos_resultado = None
        error_msg = str(e)
        with estadisticas_lock:
            estadisticas["tareas_fallidas"] += 1

    t_fin = time.perf_counter()
    duracion = round(t_fin - t_inicio, 4)
    ts_fin = datetime.now().isoformat()

    print(f"[WORKER {worker_nombre}] <- Tarea ID: {task_id[:8]} finalizada en {duracion}s | Estado: {estado}")

    return {
        "task_id": task_id,
        "tipo": tipo_tarea,
        "estado": estado,
        "worker": worker_nombre,
        "tiempo_ejecucion_seg": duracion,
        "timestamp_inicio": ts_inicio,
        "timestamp_fin": ts_fin,
        "resultado": datos_resultado,
        "error": error_msg
    }


# ------------------------------------------------------------------------------
# Protocolo de Comunicación sobre Sockets
# ------------------------------------------------------------------------------

def enviar_mensaje(sock: socket.socket, datos: dict) -> None:
    """Serializa un diccionario a JSON y lo envía con delimitador de nueva línea."""
    payload = json.dumps(datos, ensure_ascii=False) + "\n"
    sock.sendall(payload.encode('utf-8'))


def recibir_mensajes(sock: socket.socket):
    """
    Generador que procesa el flujo de bytes del socket TCP y yield-ea
    cada objeto JSON delimitado por salto de línea.
    Evita los problemas de fragmentación y stream-sticking de TCP.
    """
    buffer = ""
    while True:
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            break
        buffer += chunk.decode('utf-8')
        while "\n" in buffer:
            linea, buffer = buffer.split("\n", 1)
            linea = linea.strip()
            if linea:
                try:
                    yield json.loads(linea)
                except json.JSONDecodeError as err:
                    print(f"[-] Error decodificando JSON: {err} | Payload: {linea[:60]}")


# ------------------------------------------------------------------------------
# Manejo de Clientes Conectados
# ------------------------------------------------------------------------------

def atender_cliente(client_socket: socket.socket, client_addr: tuple, pool_workers: ThreadPoolExecutor):
    """
    Hilo dedicado a una conexión de cliente.
    Recibe tareas por socket, las envía al ThreadPoolExecutor de workers,
    y transmite de vuelta las respuestas calculadas.
    """
    ip, puerto = client_addr
    print(f"[+] [RED] Cliente conectado desde {ip}:{puerto}")
    
    with estadisticas_lock:
        estadisticas["clientes_conectados_historico"] += 1
        estadisticas["clientes_activos"] += 1

    try:
        for solicitud in recibir_mensajes(client_socket):
            comando = solicitud.get("comando", "tarea")

            if comando == "ping":
                enviar_mensaje(client_socket, {
                    "tipo": "pong",
                    "timestamp": datetime.now().isoformat(),
                    "mensaje": "Servidor y Pool de Workers operativos."
                })
                continue

            elif comando == "estado":
                with estadisticas_lock:
                    copia_stats = dict(estadisticas)
                enviar_mensaje(client_socket, {
                    "tipo": "estado_servidor",
                    "max_workers": MAX_WORKERS,
                    "estadisticas": copia_stats
                })
                continue

            elif comando == "tarea":
                # Registrar tarea recibida
                with estadisticas_lock:
                    estadisticas["tareas_recibidas"] += 1

                tarea_info = solicitud.get("tarea", {})
                if not tarea_info.get("task_id"):
                    tarea_info["task_id"] = str(uuid.uuid4())

                # ¡CLAVE DE LA DISTRIBUCIÓN!: Delegar la tarea al ThreadPool de Workers
                # Esto no bloquea la recepción de otros sockets ni la lectura del socket actual.
                futuro = pool_workers.submit(ejecutar_tarea_worker, tarea_info)
                
                # Obtener el resultado procesado por el worker
                resultado = futuro.result()

                # Responder al cliente por el socket TCP
                enviar_mensaje(client_socket, {
                    "tipo": "respuesta_tarea",
                    "datos": resultado
                })

            else:
                enviar_mensaje(client_socket, {
                    "tipo": "error",
                    "mensaje": f"Comando '{comando}' no reconocido."
                })

    except (ConnectionResetError, BrokenPipeError):
        print(f"[*] [RED] Cliente {ip}:{puerto} cerró abruptamente la conexión.")
    except Exception as e:
        print(f"[-] [RED] Error en comunicación con {ip}:{puerto}: {e}")
    finally:
        with estadisticas_lock:
            estadisticas["clientes_activos"] -= 1
        try:
            client_socket.close()
        except Exception:
            pass
        print(f"[-] [RED] Conexión finalizada con {ip}:{puerto}")


# ------------------------------------------------------------------------------
# Bucle Principal del Servidor
# ------------------------------------------------------------------------------

def iniciar_servidor(host=HOST, puerto=PORT, max_workers=MAX_WORKERS):
    """Inicializa el socket servidor y el pool de workers multihilo."""
    print("=" * 72)
    print("  PFO 3: SERVIDOR DISTRIBUIDO CON SOCKETS Y POOL DE WORKERS (TSDS)  ")
    print(f"  Escuchando en: {host}:{puerto}")
    print(f"  Workers disponibles en el Pool de Hilos: {max_workers}")
    print("=" * 72)

    # 1. Instanciar el pool de hilos para ejecución de tareas
    pool_workers = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="WorkerThread")

    # 2. Configurar socket TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, puerto))
        server_socket.listen(BACKLOG)
        print(f"[+] [INICIO] Servidor iniciado exitosamente. Presione Ctrl+C para detener.")
        
        while True:
            # 3. Aceptar nuevas conexiones entrantes de clientes
            client_sock, client_addr = server_socket.accept()
            
            # Lanzar un hilo para atender cada cliente entrante
            hilo_cliente = threading.Thread(
                target=atender_cliente,
                args=(client_sock, client_addr, pool_workers),
                daemon=True,
                name=f"Handler-{client_addr[0]}:{client_addr[1]}"
            )
            hilo_cliente.start()

    except KeyboardInterrupt:
        print("\n[*] [APAGADO] Deteniendo servidor de forma ordenada...")
    except Exception as e:
        print(f"[-] [FATAL] Error en el servidor: {e}")
    finally:
        server_socket.close()
        print("[*] [APAGADO] Cerrando Pool de Workers (esperando finalización de tareas)...")
        pool_workers.shutdown(wait=True)
        print("[+] [APAGADO] Servidor apagado limpiamente.")


if __name__ == '__main__':
    iniciar_servidor()
