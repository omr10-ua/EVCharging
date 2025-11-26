#!/usr/bin/env python3
"""
EV_CP_E - Engine del punto de recarga (standalone Python)
Se conecta a Central vía Kafka y expone un servidor socket para el Monitor.
"""

import threading
import time
import socket
import json
import os
from kafka import KafkaProducer
from utils.helper import log_message

# Configuración desde variables de entorno
CP_ID = os.getenv('CP_ID', 'CP001')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
MONITOR_PORT = int(os.getenv('MONITOR_PORT', 9101))
CENTRAL_TOPIC = os.getenv('CENTRAL_TOPIC', 'central_topic')

# Inicializar producer Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

status_flag = 'OK'
supplying = False

# Servidor socket para monitor
def monitor_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', MONITOR_PORT))
    server.listen()
    log_message(f'[ENGINE] Monitor server escuchando en puerto {MONITOR_PORT}')

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_monitor_conn, args=(conn,), daemon=True).start()


def handle_monitor_conn(conn):
    global status_flag
    try:
        data = conn.recv(1024).decode()
        if data.strip() == 'HEALTH_CHECK':
            conn.sendall(status_flag.encode())
    except Exception as e:
        log_message(f'[ENGINE] Error monitor: {e}')
    finally:
        conn.close()

# Listener de teclado para simular averías o suministro
def keyboard_listener():
    global status_flag, supplying
    log_message('[ENGINE] Controles: f = toggle avería, p = iniciar suministro, u = parar suministro, q = salir')
    while True:
        key = input().strip().lower()
        if key == 'f':
            status_flag = 'KO' if status_flag == 'OK' else 'OK'
            log_message(f'[ENGINE] Estado avería cambiado a {status_flag}')
        elif key == 'p':
            supplying = True
            log_message('[ENGINE] Suministro iniciado')
        elif key == 'u':
            supplying = False
            log_message('[ENGINE] Suministro finalizado')
        elif key == 'q':
            log_message('[ENGINE] Saliendo...')
            os._exit(0)

# Loop de telemetría hacia central
def telemetry_loop():
    global supplying
    while True:
        if supplying:
            msg = {'cp_id': CP_ID, 'status': 'SUPPLYING', 'timestamp': time.time()}
            producer.send(CENTRAL_TOPIC, msg)
            log_message(f'[ENGINE] Telemetría enviada: {msg}')
        time.sleep(1)

if __name__ == '__main__':
    threading.Thread(target=monitor_server, daemon=True).start()
    threading.Thread(target=keyboard_listener, daemon=True).start()
    threading.Thread(target=telemetry_loop, daemon=True).start()

    while True:
        time.sleep(1)