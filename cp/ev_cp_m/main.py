#!/usr/bin/env python3
"""
EV_CP_M - Monitor del punto de recarga (CORREGIDO)
Se conecta al Engine por sockets y a Central (puerto 5001) por sockets 
para autenticación y reporte de estado de salud.
"""

import socket
import time
import os
import json
import sys

# Añadir path para imports relativos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.helper import log_message

# Configuración desde variables de entorno
CP_ID = os.getenv('CP_ID', 'CP001')
CP_LOCATION = os.getenv('CP_LOCATION', 'Valencia Centro')
CP_PRICE = float(os.getenv('CP_PRICE', '0.35'))

ENGINE_HOST = os.getenv('ENGINE_HOST', 'localhost')
ENGINE_MONITOR_PORT = int(os.getenv('ENGINE_MONITOR_PORT', 9101))

CENTRAL_HOST = os.getenv('CENTRAL_HOST', 'localhost')
CENTRAL_PORT = int(os.getenv('CENTRAL_PORT', 5001))  # ✅ CORREGIDO: 9000 → 5001

sock_central = None
registered = False

def connect_to_central():
    """Conecta a Central y registra el CP"""
    global sock_central, registered
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            sock_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_central.settimeout(5)
            sock_central.connect((CENTRAL_HOST, CENTRAL_PORT))
            
            # ✅ CORREGIDO: Enviar registro en formato JSON
            register_msg = {
                "type": "register",
                "cp_id": CP_ID,
                "location": CP_LOCATION,
                "price": CP_PRICE
            }
            
            msg_str = json.dumps(register_msg) + "\n"
            sock_central.sendall(msg_str.encode('utf-8'))
            
            # Esperar ACK
            response = sock_central.recv(4096).decode('utf-8').strip()
            if response:
                ack = json.loads(response)
                if ack.get("type") == "register_ack" and ack.get("status") == "ok":
                    log_message(f'[MONITOR] ✅ Registrado en CENTRAL: {CP_ID} @ {CP_LOCATION} (€{CP_PRICE}/kWh)')
                    registered = True
                    return True
            
            log_message('[MONITOR] ⚠️  No se recibió ACK correcto')
            sock_central.close()
            
        except Exception as e:
            log_message(f'[MONITOR] ❌ Error conectando a CENTRAL (intento {retry_count+1}/{max_retries}): {e}')
            if sock_central:
                sock_central.close()
        
        retry_count += 1
        time.sleep(2)
    
    log_message('[MONITOR] ❌ No se pudo conectar a CENTRAL después de varios intentos')
    return False

def send_to_central(message_dict):
    """Envía un mensaje JSON a Central"""
    global sock_central
    
    try:
        msg_str = json.dumps(message_dict) + "\n"
        sock_central.sendall(msg_str.encode('utf-8'))
        return True
    except Exception as e:
        log_message(f'[MONITOR] Error enviando mensaje a Central: {e}')
        return False

def check_engine_health():
    """Comprueba el estado de salud del Engine"""
    try:
        sock_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_engine.settimeout(2)
        sock_engine.connect((ENGINE_HOST, ENGINE_MONITOR_PORT))
        sock_engine.sendall(b'HEALTH_CHECK')
        status = sock_engine.recv(1024).decode().strip()
        sock_engine.close()
        
        return status
        
    except Exception as e:
        log_message(f'[MONITOR] ⚠️  No se pudo conectar con Engine: {e}')
        return "DISCONNECTED"

def monitoring_loop():
    """Loop principal de monitorización"""
    global sock_central, registered
    
    last_status = "OK"
    forced_stop = False  # ✅ Flag para comandos de parar
    
    while True:
        try:
            # Verificar si seguimos conectados a Central
            if not registered or sock_central is None:
                log_message('[MONITOR] Intentando reconectar a CENTRAL...')
                if not connect_to_central():
                    time.sleep(5)
                    continue
            
            # ✅ NUEVO: Verificar si hay comandos de Central (non-blocking)
            try:
                sock_central.settimeout(0.1)  # timeout corto para no bloquear
                data = sock_central.recv(4096)
                if data:
                    try:
                        msg = json.loads(data.decode('utf-8').strip())
                        if msg.get("type") == "command":
                            action = msg.get("action")
                            if action == "stop":
                                forced_stop = True
                                log_message('[MONITOR] 🛑 Comando PARAR recibido de CENTRAL')
                                
                                # ✅ Notificar estado PARADO a Central
                                status_msg = {
                                    "type": "status_change",
                                    "cp_id": CP_ID,
                                    "state": "PARADO"
                                }
                                send_to_central(status_msg)
                                
                                # Enviar ACK
                                ack = {"type": "command_ack", "cp_id": CP_ID, "action": "stop", "status": "ok"}
                                sock_central.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                                
                            elif action == "resume":
                                forced_stop = False
                                log_message('[MONITOR] ▶️  Comando REANUDAR recibido de CENTRAL')
                                
                                # ✅ Notificar estado ACTIVADO a Central
                                status_msg = {
                                    "type": "status_change",
                                    "cp_id": CP_ID,
                                    "state": "ACTIVADO"
                                }
                                send_to_central(status_msg)
                                
                                # Enviar ACK
                                ack = {"type": "command_ack", "cp_id": CP_ID, "action": "resume", "status": "ok"}
                                sock_central.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                    except json.JSONDecodeError:
                        pass
            except socket.timeout:
                pass  # No hay datos, continuar
            
            # Si está forzado a parar, no hacer health checks, reportar como PARADO
            if forced_stop:
                time.sleep(1)
                continue
            
            # Comprobar salud del Engine
            engine_status = check_engine_health()
            
            # Solo enviar si hay cambio de estado
            if engine_status != last_status:
                if engine_status == "KO":
                    fault_msg = {
                        "type": "fault",
                        "cp_id": CP_ID,
                        "msg": "Engine reportó estado KO"
                    }
                    if send_to_central(fault_msg):
                        log_message('[MONITOR] 🔴 AVERÍA detectada, notificado a CENTRAL')
                        last_status = "KO"
                
                elif engine_status == "DISCONNECTED":
                    disconnect_msg = {
                        "type": "fault",
                        "cp_id": CP_ID,
                        "msg": "No se puede conectar con Engine"
                    }
                    if send_to_central(disconnect_msg):
                        log_message('[MONITOR] 🔴 Engine DESCONECTADO, notificado a CENTRAL')
                        last_status = "DISCONNECTED"
                
                elif engine_status == "OK" and last_status != "OK":
                    # Recuperación de fallo
                    recovery_msg = {
                        "type": "register",  # Re-registrar para volver a ACTIVADO
                        "cp_id": CP_ID,
                        "location": CP_LOCATION,
                        "price": CP_PRICE
                    }
                    if send_to_central(recovery_msg):
                        log_message('[MONITOR] 🟢 Engine recuperado, notificado a CENTRAL')
                        last_status = "OK"
            
        except KeyboardInterrupt:
            log_message('[MONITOR] Saliendo...')
            # Notificar desconexión
            try:
                disconnect_msg = {
                    "type": "disconnect",
                    "cp_id": CP_ID
                }
                send_to_central(disconnect_msg)
                sock_central.close()
            except:
                pass
            break
        
        except Exception as e:
            log_message(f'[MONITOR] Error en loop principal: {e}')
            registered = False
            if sock_central:
                sock_central.close()
                sock_central = None
        
        time.sleep(1)  # Check cada segundo según especificación

if __name__ == '__main__':
    log_message(f'[MONITOR] Iniciando Monitor para CP: {CP_ID}')
    log_message(f'[MONITOR] Engine: {ENGINE_HOST}:{ENGINE_MONITOR_PORT}')
    log_message(f'[MONITOR] Central: {CENTRAL_HOST}:{CENTRAL_PORT}')
    
    monitoring_loop()