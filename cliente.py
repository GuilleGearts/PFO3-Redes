"""
================================================================================
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Materia: Programación sobre Redes - Tecnicatura Superior en Desarrollo de Software (TSDS)
Alumno: Guillermo Aybar
Año: 2026

Archivo: cliente.py
Descripción:
    Cliente de red basado en sockets TCP que implementa:
    - Conexión con reintentos y tolerancia a fallos (según Clase 10).
    - Protocolo de envío de tareas en JSON y recepción síncrona/asíncrona de resultados.
    - Menú interactivo en consola y modo de prueba automatizada en lote.
================================================================================
"""

import socket
import json
import time
import uuid
import sys
from datetime import datetime

# ------------------------------------------------------------------------------
# Configuración del Cliente
# ------------------------------------------------------------------------------
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 65432
BUFFER_SIZE = 4096
MAX_REINTENTOS = 4
ESPERA_REINTENTO = 2  # Segundos entre intentos


# ------------------------------------------------------------------------------
# Manejo de Conexión con Reintentos (Tolerancia a Fallos - Clase 10)
# ------------------------------------------------------------------------------

def conectar_con_reintentos(host=SERVER_HOST, puerto=SERVER_PORT, max_intentos=MAX_REINTENTOS, espera=ESPERA_REINTENTO):
    """
    Establece conexión TCP con el servidor aplicando una política de reintentos
    para tolerar caídas temporales de la red o demoras en el arranque del servidor.
    (Basado en las directrices de la Clase 10 - Manejo de Errores en Sockets).
    """
    for intento in range(1, max_intentos + 1):
        try:
            print(f"[*] [INTENTO {intento}/{max_intentos}] Conectando a {host}:{puerto}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)  # Timeout de 10 segundos para operaciones de socket
            sock.connect((host, puerto))
            print(f"[+] [ÉXITO] Conexión establecida con el servidor distribuido en {host}:{puerto}.\n")
            return sock
        except (socket.error, ConnectionRefusedError, TimeoutError) as e:
            print(f"[-] [FALLO] No se pudo conectar: {e}")
            if intento < max_intentos:
                print(f"[*] Esperando {espera} segundos antes de reintentar...\n")
                time.sleep(espera)
            else:
                print(f"[!] [ERROR CRÍTICO] Se agotaron los {max_intentos} intentos de conexión.")
    return None


# ------------------------------------------------------------------------------
# Protocolo de Mensajería
# ------------------------------------------------------------------------------

def enviar_y_recibir(sock: socket.socket, solicitud: dict) -> dict:
    """Envía un mensaje JSON terminado en nueva línea y lee la respuesta JSON."""
    payload = json.dumps(solicitud, ensure_ascii=False) + "\n"
    sock.sendall(payload.encode('utf-8'))

    # Leer respuesta hasta encontrar el salto de línea
    buffer = ""
    while "\n" not in buffer:
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            raise ConnectionError("El servidor cerró la conexión inesperadamente.")
        buffer += chunk.decode('utf-8')

    linea = buffer.strip().split("\n")[0]
    return json.loads(linea)


# ------------------------------------------------------------------------------
# Funciones de Envío de Tareas
# ------------------------------------------------------------------------------

def formatear_resultado_tarea(datos: dict):
    """Imprime en consola el resultado de una tarea de manera estructurada."""
    print("\n" + "=" * 65)
    print(f"  RESULTADO DE TAREA DISTRIBUIDA (ID: {datos.get('task_id')})")
    print("=" * 65)
    print(f"  * Tipo de Tarea     : {datos.get('tipo')}")
    print(f"  * Estado            : {datos.get('estado')}")
    print(f"  * Worker Asignado   : {datos.get('worker')}")
    print(f"  * Tiempo de Computo : {datos.get('tiempo_ejecucion_seg')} segundos")
    print(f"  * Inicio / Fin      : {datos.get('timestamp_inicio')} -> {datos.get('timestamp_fin')}")
    
    if datos.get("error"):
        print(f"  * Error reportado   : {datos.get('error')}")
    else:
        print("  * Datos Procesados  :")
        for clave, valor in datos.get("resultado", {}).items():
            print(f"      - {clave}: {valor}")
    print("=" * 65 + "\n")


def enviar_tarea(sock: socket.socket, tipo: str, parametros: dict) -> dict:
    """Construye el paquete de solicitud de tarea y lo envía al servidor."""
    task_id = str(uuid.uuid4())
    solicitud = {
        "comando": "tarea",
        "tarea": {
            "task_id": task_id,
            "tipo": tipo,
            "parametros": parametros
        }
    }
    
    t_inicio = time.perf_counter()
    respuesta = enviar_y_recibir(sock, solicitud)
    t_total = round(time.perf_counter() - t_inicio, 4)
    
    if respuesta.get("tipo") == "respuesta_tarea":
        datos = respuesta.get("datos", {})
        datos["latencia_red_total"] = t_total
        return datos
    else:
        return {"estado": "ERROR", "mensaje": str(respuesta)}


# ------------------------------------------------------------------------------
# Flujos de Pruebas y Menú Interactivo
# ------------------------------------------------------------------------------

def ejecutar_prueba_matematica(sock):
    """Pide parámetros al usuario para cálculo de primos."""
    print("\n--- [1] Tarea: Cálculo de Números Primos (CPU Intensivo) ---")
    try:
        limite_input = input("Ingrese el número límite a evaluar (Enter para 10000): ").strip()
        limite = int(limite_input) if limite_input else 10000
    except ValueError:
        limite = 10000

    print(f"[*] Enviando tarea al clúster para calcular primos hasta {limite}...")
    res = enviar_tarea(sock, "calculo_primos", {"limite": limite})
    formatear_resultado_tarea(res)


def ejecutar_prueba_criptografica(sock):
    """Pide parámetros para hashing iterativo."""
    print("\n--- [2] Tarea: Hashing Criptográfico Iterativo (Key Stretching) ---")
    texto = input("Texto o contraseña a hashear (Enter para 'PasswordSegura2026!'): ").strip()
    if not texto:
        texto = "PasswordSegura2026!"
    try:
        rondas_input = input("Rondas de cálculo (Enter para 60000): ").strip()
        rondas = int(rondas_input) if rondas_input else 60000
    except ValueError:
        rondas = 60000

    print(f"[*] Enviando tarea criptográfica con {rondas} rondas de SHA-256...")
    res = enviar_tarea(sock, "hash_criptografico", {"texto": texto, "rondas": rondas})
    formatear_resultado_tarea(res)


def ejecutar_prueba_texto(sock):
    """Pide un texto a procesar."""
    print("\n--- [3] Tarea: Procesamiento y Minería de Texto ---")
    texto_defecto = (
        "La arquitectura distribuida permite desacoplar los clientes de los servidores. "
        "En un sistema distribuido con sockets y workers, la concurrencia mejora la latencia "
        "y permite que múltiples hilos procesen peticiones en paralelo sin saturar el sistema."
    )
    print(f"Texto por defecto:\n\"{texto_defecto}\"")
    elegir = input("¿Usar texto por defecto? (S/n): ").strip().lower()
    if elegir == 'n':
        texto = input("Ingrese su texto: ").strip()
    else:
        texto = texto_defecto

    print("[*] Enviando tarea de análisis de texto al pool de workers...")
    res = enviar_tarea(sock, "procesamiento_texto", {"texto": texto})
    formatear_resultado_tarea(res)


def ejecutar_prueba_almacenamiento_s3(sock):
    """Simula el pipeline S3 + PostgreSQL."""
    print("\n--- [4] Tarea: Pipeline de Almacenamiento Distribuido (S3 + PostgreSQL) ---")
    archivo = input("Nombre del archivo de datos a persistir (Enter para 'reporte_ventas_2026.csv'): ").strip()
    if not archivo:
        archivo = "reporte_ventas_2026.csv"
    
    print(f"[*] Enviando solicitud de procesamiento y persistencia para '{archivo}'...")
    res = enviar_tarea(sock, "almacenamiento_s3", {"nombre_archivo": archivo, "tamano_kb": 1024})
    formatear_resultado_tarea(res)


def consultar_estado_servidor(sock):
    """Consulta métricas al servidor."""
    print("\n--- [5] Consulta de Métricas del Clúster / Servidor ---")
    solicitud = {"comando": "estado"}
    respuesta = enviar_y_recibir(sock, solicitud)
    stats = respuesta.get("estadisticas", {})
    print("=" * 55)
    print("  ESTADO DEL SERVIDOR DISTRIBUIDO")
    print("=" * 55)
    print(f"  * Workers en el ThreadPool : {respuesta.get('max_workers')}")
    print(f"  * Servidor iniciado en     : {stats.get('servidor_iniciado')}")
    print(f"  * Clientes activos         : {stats.get('clientes_activos')}")
    print(f"  * Clientes historicos      : {stats.get('clientes_conectados_historico')}")
    print(f"  * Tareas recibidas         : {stats.get('tareas_recibidas')}")
    print(f"  * Tareas completadas       : {stats.get('tareas_completadas')}")
    print(f"  * Tareas fallidas          : {stats.get('tareas_fallidas')}")
    print("=" * 55 + "\n")


def ejecutar_lote_concurrente(sock):
    """Envía un lote secuencial de diferentes tareas para verificar la versatilidad del pool."""
    print("\n--- [6] Demostración de Envío de Lote de Tareas Variadas ---")
    tareas_lote = [
        ("calculo_primos", {"limite": 8000}),
        ("hash_criptografico", {"texto": "TokenAuth_1", "rondas": 30000}),
        ("procesamiento_texto", {"texto": "Sistemas distribuidos de alta disponibilidad y tolerancia a fallos."}),
        ("almacenamiento_s3", {"nombre_archivo": "backup_db_part1.tar.gz", "tamano_kb": 4096}),
        ("calculo_primos", {"limite": 12000})
    ]

    print(f"[*] Transmitiendo lote de {len(tareas_lote)} tareas hacia el servidor...")
    inicio = time.perf_counter()
    for idx, (tipo, params) in enumerate(tareas_lote, start=1):
        print(f"  -> Enviando tarea {idx}/{len(tareas_lote)}: '{tipo}'...")
        res = enviar_tarea(sock, tipo, params)
        print(f"     [OK] Worker: {res.get('worker')} | Tiempo: {res.get('tiempo_ejecucion_seg')}s | Estado: {res.get('estado')}")
    
    total = round(time.perf_counter() - inicio, 4)
    print(f"\n[+] Lote finalizado con éxito en {total} segundos totales de ida y vuelta.\n")


def menu_interactivo():
    """Bucle principal de la interfaz de consola del cliente."""
    sock = conectar_con_reintentos()
    if not sock:
        print("[-] No se pudo establecer comunicación con el servidor. Saliendo.")
        sys.exit(1)

    try:
        while True:
            print("=================================================================")
            print("  PFO 3: CLIENTE DE SISTEMA DISTRIBUIDO (SOCKETS TCP)            ")
            print("  Alumno: Guillermo Aybar | TSDS 2026                            ")
            print("=================================================================")
            print("  1. Enviar Tarea: Cálculo Matemático (Números Primos en Rango)")
            print("  2. Enviar Tarea: Criptografía Intensiva (Hashing con Salt)")
            print("  3. Enviar Tarea: Procesamiento y Análisis de Texto")
            print("  4. Enviar Tarea: Pipeline Distribuido (S3 + PostgreSQL)")
            print("  5. Consultar Métricas y Estado del Clúster / Servidor")
            print("  6. Enviar Lote de Demostración de Tareas Variadas")
            print("  7. Salir")
            print("=================================================================")
            
            opcion = input("Seleccione una opción (1-7): ").strip()
            
            if opcion == '1':
                ejecutar_prueba_matematica(sock)
            elif opcion == '2':
                ejecutar_prueba_criptografica(sock)
            elif opcion == '3':
                ejecutar_prueba_texto(sock)
            elif opcion == '4':
                ejecutar_prueba_almacenamiento_s3(sock)
            elif opcion == '5':
                consultar_estado_servidor(sock)
            elif opcion == '6':
                ejecutar_lote_concurrente(sock)
            elif opcion == '7':
                print("[*] Cerrando conexión con el servidor. ¡Hasta luego!")
                break
            else:
                print("[!] Opción inválida. Por favor seleccione un número del 1 al 7.\n")

    except KeyboardInterrupt:
        print("\n[*] Interrupción de usuario. Cerrando cliente...")
    except Exception as e:
        print(f"\n[-] Error de comunicación: {e}")
    finally:
        try:
            sock.close()
        except Exception:
            pass


if __name__ == '__main__':
    # Modo automático si se invoca con flag --auto o --test
    if len(sys.argv) > 1 and sys.argv[1] in ("--auto", "--test"):
        print("[*] Ejecutando Cliente en Modo Automatizado...")
        s = conectar_con_reintentos()
        if s:
            ejecutar_lote_concurrente(s)
            consultar_estado_servidor(s)
            s.close()
    else:
        menu_interactivo()
