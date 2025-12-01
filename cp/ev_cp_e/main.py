#!/usr/bin/env python3
"""
EV_CP_E - Engine del punto de recarga (MEJORADO)
Se conecta a Central vía Kafka para telemetría en tiempo real.
Expone servidor socket para Monitor (health checks).
Simula consumo realista de carga de vehículo eléctrico.
"""

import threading
import time
import socket
import json
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Añadir path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.helper import log_message

# Configuración desde variables de entorno
CP_ID = os.getenv('CP_ID', 'CP001')
CP_LOCATION = os.getenv('CP_LOCATION', 'Valencia Centro')
CP_PRICE = float(os.getenv('CP_PRICE', '0.35'))

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
MONITOR_PORT = int(os.getenv('MONITOR_PORT', 9101))
KAFKA_TOPIC_TELEMETRY = os.getenv('KAFKA_TOPIC_TELEMETRY', 'cp_telemetry')

# Estado del CP
status_flag = 'OK'  # OK / KO
supplying = False
current_driver = None
total_kwh = 0.0
current_kw = 0.0

# Simulación de carga realista
CHARGE_POWER_MAX = 22.0  # kW (cargador típico AC)
CHARGE_INCREMENT = 0.5   # Incremento por segundo

# Inicializar producer Kafka (con reintentos)
def create_kafka_producer():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                max_block_ms=5000
            )
            log_message(f'[ENGINE] ✅ Conectado a Kafka: {KAFKA_BROKER}')
            return producer
        except Exception as e:
            log_message(f'[ENGINE] ⚠️  Error conectando a Kafka (intento {attempt+1}/{max_retries}): {e}')
            time.sleep(2)
    
    log_message('[ENGINE] ❌ No se pudo conectar a Kafka')
    return None

producer = create_kafka_producer()

# Servidor socket para monitor
def monitor_server():
    """Servidor que responde a health checks del Monitor"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', MONITOR_PORT))
    server.listen(5)
    log_message(f'[ENGINE] 🔌 Monitor server escuchando en puerto {MONITOR_PORT}')

    while True:
        try:
            conn, addr = server.accept()
            threading.Thread(target=handle_monitor_conn, args=(conn,), daemon=True).start()
        except Exception as e:
            log_message(f'[ENGINE] Error en monitor server: {e}')


def handle_monitor_conn(conn):
    """Maneja conexión del Monitor (health check)"""
    global status_flag
    try:
        data = conn.recv(1024).decode().strip()
        if data == 'HEALTH_CHECK':
            response = status_flag  # OK o KO
            conn.sendall(response.encode())
    except Exception as e:
        log_message(f'[ENGINE] Error en health check: {e}')
    finally:
        conn.close()


def keyboard_listener():
    """Listener de teclado para controlar simulación"""
    global status_flag, supplying, current_driver
    
    print("\n" + "="*60)
    print("🎮 CONTROLES DEL CP ENGINE")
    print("="*60)
    print("  f  = Toggle avería (OK ↔ KO)")
    print("  p  = Iniciar suministro (simula enchufar vehículo)")
    print("  u  = Parar suministro (simula desenchufar)")
    print("  q  = Salir")
    print("="*60 + "\n")
    
    while True:
        try:
            key = input().strip().lower()
            
            if key == 'f':
                status_flag = 'KO' if status_flag == 'OK' else 'OK'
                symbol = '🔴' if status_flag == 'KO' else '🟢'
                log_message(f'[ENGINE] {symbol} Estado cambiado a {status_flag}')
                
            elif key == 'p':
                if not supplying:
                    # Simular que un conductor solicitó servicio
                    current_driver = f"DRV{int(time.time()) % 10000}"
                    supplying = True
                    log_message(f'[ENGINE] 🔌 Suministro iniciado para conductor {current_driver}')
                else:
                    log_message('[ENGINE] ⚠️  Ya hay un suministro en curso')
                    
            elif key == 'u':
                if supplying:
                    supplying = False
                    log_message(f'[ENGINE] 🔋 Suministro finalizado. Total: {total_kwh:.2f} kWh (€{total_kwh * CP_PRICE:.2f})')
                    current_driver = None
                else:
                    log_message('[ENGINE] ⚠️  No hay suministro activo')
                    
            elif key == 'q':
                log_message('[ENGINE] 👋 Saliendo...')
                os._exit(0)
                
        except EOFError:
            time.sleep(0.1)
        except Exception as e:
            log_message(f'[ENGINE] Error en keyboard listener: {e}')


def telemetry_loop():
    """Loop que envía telemetría a Central vía Kafka"""
    global supplying, total_kwh, current_kw, producer
    
    last_send = 0
    
    while True:
        try:
            # Si estamos suministrando, simular carga progresiva
            if supplying and status_flag == 'OK':
                # Incrementar potencia hasta máximo
                if current_kw < CHARGE_POWER_MAX:
                    current_kw = min(current_kw + CHARGE_INCREMENT, CHARGE_POWER_MAX)
                
                # Acumular energía (kWh = kW * horas, con dt=1seg)
                total_kwh += current_kw / 3600.0  # 1 segundo = 1/3600 hora
                
            else:
                # Si no suministramos, reset
                if not supplying:
                    current_kw = 0.0
                    total_kwh = 0.0
            
            # Enviar telemetría cada segundo cuando suministra, cada 5 cuando no
            now = time.time()
            send_interval = 1 if supplying else 5
            
            if now - last_send >= send_interval:
                msg = {
                    'type': 'telemetry',
                    'cp_id': CP_ID,
                    'is_supplying': supplying,
                    'consumption_kw': round(current_kw, 2),
                    'total_kwh': round(total_kwh, 3),
                    'current_price': CP_PRICE,
                    'driver_id': current_driver,
                    'timestamp': now
                }
                
                if producer:
                    try:
                        producer.send(KAFKA_TOPIC_TELEMETRY, msg)
                        
                        if supplying:
                            euros = total_kwh * CP_PRICE
                            log_message(f'[ENGINE] 📊 Telemetría: {current_kw:.1f} kW | {total_kwh:.2f} kWh | €{euros:.2f}')
                    except KafkaError as e:
                        log_message(f'[ENGINE] ❌ Error enviando telemetría: {e}')
                
                last_send = now
            
        except Exception as e:
            log_message(f'[ENGINE] Error en telemetry loop: {e}')
        
        time.sleep(0.5)  # Loop rápido para simulación suave


if __name__ == '__main__':
    log_message('='*60)
    log_message(f'[ENGINE] 🚗 Iniciando CP Engine: {CP_ID}')
    log_message(f'[ENGINE] 📍 Ubicación: {CP_LOCATION}')
    log_message(f'[ENGINE] 💰 Precio: €{CP_PRICE}/kWh')
    log_message(f'[ENGINE] 📡 Kafka: {KAFKA_BROKER}')
    log_message('='*60)
    
    # Iniciar threads
    threading.Thread(target=monitor_server, daemon=True).start()
    threading.Thread(target=keyboard_listener, daemon=True).start()
    threading.Thread(target=telemetry_loop, daemon=True).start()

    # Mantener vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log_message('[ENGINE] Interrupt recibido, saliendo...')
