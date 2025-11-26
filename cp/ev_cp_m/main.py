#!/usr/bin/env python3
"""
EV_CP_M - Monitor del punto de recarga (standalone Python)
Se conecta al Engine por sockets y a Central por sockets para autenticación y reporte de estado de salud.
"""

import socket
import time
import os
from utils.helper import log_message

# Configuración desde variables de entorno
CP_ID = os.getenv('CP_ID', 'CP001')
ENGINE_HOST = os.getenv('ENGINE_HOST', 'localhost')
ENGINE_MONITOR_PORT = int(os.getenv('ENGINE_MONITOR_PORT', 9101))
CENTRAL_HOST = os.getenv('CENTRAL_HOST', 'localhost')
CENTRAL_PORT = int(os.getenv('CENTRAL_PORT', 9000))

# Conectar a Central para autenticación
try:
    sock_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_central.connect((CENTRAL_HOST, CENTRAL_PORT))
    sock_central.sendall(f'AUTH:{CP_ID}\n'.encode())
    log_message(f'[MONITOR] Autenticado en CENTRAL con CP {CP_ID}')
except Exception as e:
    log_message(f'[MONITOR] ERROR al conectar con CENTRAL: {e}')
    exit(1)

# Loop principal de monitorización
while True:
    try:
        sock_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_engine.settimeout(1)
        sock_engine.connect((ENGINE_HOST, ENGINE_MONITOR_PORT))
        sock_engine.sendall(b'HEALTH_CHECK')
        status = sock_engine.recv(1024).decode()

        if status == 'KO':
            sock_central.sendall(f'FAIL:{CP_ID}\n'.encode())
            log_message('[MONITOR] AVERÍA detectada, notificando CENTRAL')
        sock_engine.close()

    except Exception:
        sock_central.sendall(f'FAIL:{CP_ID}\n'.encode())
        log_message('[MONITOR] ERROR conexión con Engine, notificando CENTRAL')

    time.sleep(1)
