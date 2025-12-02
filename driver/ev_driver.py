#!/usr/bin/env python3
"""
EV_Driver - Aplicación del Conductor
Permite solicitar servicios de recarga en puntos de carga
"""

import os
import sys
import socket
import json
import time
import threading
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ==================== CONFIGURACIÓN ====================

DRIVER_ID = os.environ.get('DRIVER_ID', 'DRV001')
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:29092')
CENTRAL_HOST = os.environ.get('CENTRAL_HOST', 'localhost')
CENTRAL_PORT = int(os.environ.get('CENTRAL_PORT', 5001))

# ==================== ESTADO GLOBAL ====================

current_service = {
    'active': False,
    'cp_id': None,
    'start_time': None,
    'total_kwh': 0.0,
    'total_euros': 0.0,
    'current_kw': 0.0
}

service_lock = threading.Lock()

# ==================== UTILIDADES ====================

def log_message(msg):
    """Imprime mensaje con timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")

def clear_screen():
    """Limpia la pantalla"""
    os.system('cls' if os.name == 'nt' else 'clear')

# ==================== COMUNICACIÓN CON CENTRAL ====================

def request_service(cp_id):
    """
    Solicita un servicio de recarga a Central
    Returns: (success: bool, message: str)
    """
    try:
        log_message(f'[DRIVER] Solicitando servicio en {cp_id}...')
        
        # Conectar a Central
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((CENTRAL_HOST, CENTRAL_PORT))
        
        # Enviar solicitud
        request = {
            "type": "service_request",
            "driver_id": DRIVER_ID,
            "cp_id": cp_id
        }
        
        message = json.dumps(request) + "\n"
        sock.sendall(message.encode('utf-8'))
        
        # Esperar respuesta
        response_data = sock.recv(4096)
        response = json.loads(response_data.decode('utf-8').strip())
        
        sock.close()
        
        if response.get('status') == 'authorized':
            log_message(f'[DRIVER] ✅ Servicio AUTORIZADO en {cp_id}')
            return True, "Autorizado"
        else:
            reason = response.get('reason', 'Motivo desconocido')
            log_message(f'[DRIVER] ❌ Servicio DENEGADO: {reason}')
            return False, reason
            
    except socket.timeout:
        log_message('[DRIVER] ❌ Timeout conectando con Central')
        return False, "Timeout"
    except Exception as e:
        log_message(f'[DRIVER] ❌ Error solicitando servicio: {e}')
        return False, str(e)

# ==================== KAFKA CONSUMER ====================

def kafka_consumer_loop():
    """
    Thread que escucha notificaciones de Kafka sobre el suministro
    """
    log_message('[KAFKA] Conectando a Kafka...')
    
    try:
        consumer = KafkaConsumer(
            'cp_telemetry',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'driver_{DRIVER_ID}'
        )
        
        log_message('[KAFKA] ✅ Conectado a Kafka')
        
        for message in consumer:
            try:
                data = message.value
                msg_type = data.get('type')
                msg_driver_id = data.get('driver_id')
                
                # Solo procesar mensajes para este driver
                if msg_driver_id != DRIVER_ID:
                    continue
                
                if msg_type == 'telemetry':
                    process_telemetry(data)
                elif msg_type == 'supply_start':
                    process_supply_start(data)
                elif msg_type == 'supply_end':
                    process_supply_end(data)
                    
            except Exception as e:
                log_message(f'[KAFKA] Error procesando mensaje: {e}')
                
    except Exception as e:
        log_message(f'[KAFKA] ❌ Error en consumer: {e}')

def process_supply_start(data):
    """Procesa inicio de suministro"""
    with service_lock:
        current_service['active'] = True
        current_service['cp_id'] = data.get('cp_id')
        current_service['start_time'] = time.time()
        current_service['total_kwh'] = 0.0
        current_service['total_euros'] = 0.0
        current_service['current_kw'] = 0.0
    
    log_message(f'[DRIVER] 🔋 Suministro INICIADO en {data.get("cp_id")}')

def process_telemetry(data):
    """Procesa telemetría en tiempo real"""
    with service_lock:
        if current_service['active']:
            current_service['current_kw'] = data.get('consumption_kw', 0.0)
            current_service['total_kwh'] = data.get('total_kwh', 0.0)
            current_service['total_euros'] = data.get('total_kwh', 0.0) * data.get('current_price', 0.0)

def process_supply_end(data):
    """Procesa fin de suministro"""
    reason = data.get('reason', 'completed')
    total_kwh = data.get('total_kwh', 0.0)
    total_euros = data.get('total_euros', 0.0)
    cp_id = data.get('cp_id', 'UNKNOWN')
    
    elapsed_time = 0
    with service_lock:
        if current_service['start_time']:
            elapsed_time = time.time() - current_service['start_time']
        current_service['active'] = False
        current_service['total_kwh'] = total_kwh
        current_service['total_euros'] = total_euros
    
    # Calcular tiempo
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    
    # Imprimir ticket
    print("\n" + "="*60)
    print("🧾 TICKET DE RECARGA")
    print("="*60)
    print(f"Conductor: {DRIVER_ID}")
    print(f"Punto de recarga: {cp_id}")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*60)
    
    if reason == 'completed':
        print("Estado: ✅ COMPLETADO")
        log_message(f'[DRIVER] ✅ Suministro COMPLETADO')
    elif reason == 'driver_disconnected':
        print("Estado: 🛑 CANCELADO (Driver desconectado)")
        log_message(f'[DRIVER] 🛑 Suministro CANCELADO por desconexión')
    elif reason == 'fault':
        print("Estado: ⚠️  INTERRUMPIDO (Avería)")
        log_message(f'[DRIVER] ⚠️  Suministro INTERRUMPIDO por avería')
    else:
        print(f"Estado: ⚠️  FINALIZADO ({reason})")
        log_message(f'[DRIVER] ⚠️  Suministro FINALIZADO: {reason}')
    
    print("-"*60)
    print(f"⚡ Energía suministrada: {total_kwh:.2f} kWh")
    print(f"💶 Importe total: €{total_euros:.2f}")
    print(f"⏱️  Tiempo de recarga: {minutes:02d}:{seconds:02d}")
    print("="*60)
    print()

# ==================== MODO INTERACTIVO ====================

def show_menu():
    """Muestra el menú principal"""
    clear_screen()
    print("="*60)
    print("🚗 EV DRIVER - Sistema de Recarga")
    print("="*60)
    print(f"Conductor: {DRIVER_ID}")
    print(f"Kafka: {KAFKA_BROKER}")
    print(f"Central: {CENTRAL_HOST}:{CENTRAL_PORT}")
    print("="*60)
    print()
    print("[1] Solicitar recarga en un CP")
    print("[2] Cargar servicios desde archivo")
    print("[3] Ver estado de mi recarga actual")
    print("[4] Salir")
    print()

def interactive_mode():
    """Modo interactivo"""
    while True:
        show_menu()
        option = input("Selecciona una opción: ").strip()
        
        if option == '1':
            request_single_service()
        elif option == '2':
            request_services_from_file()
        elif option == '3':
            show_current_service()
        elif option == '4':
            log_message('[DRIVER] Saliendo...')
            sys.exit(0)
        else:
            print("Opción inválida")
            time.sleep(1)

def request_single_service():
    """Solicita un servicio manualmente"""
    cp_id = input("\nIntroduce el ID del CP (ej: CP001): ").strip()
    
    if not cp_id:
        print("ID inválido")
        time.sleep(2)
        return
    
    success, message = request_service(cp_id)
    
    if success:
        print(f"\n✅ Servicio autorizado en {cp_id}")
        print("Esperando inicio del suministro...")
        print("(Presiona ENTER para volver al menú)")
        
        # Esperar hasta que termine o usuario presione ENTER
        wait_for_service_or_input()
    else:
        print(f"\n❌ Servicio denegado: {message}")
        input("Presiona ENTER para continuar...")

def show_current_service():
    """Muestra el estado actual del servicio"""
    with service_lock:
        if not current_service['active']:
            print("\n📭 No hay ningún servicio activo en este momento")
            input("\nPresiona ENTER para continuar...")
            return
        
        elapsed = time.time() - current_service['start_time']
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        clear_screen()
        print("="*60)
        print("🔋 RECARGA EN PROGRESO")
        print("="*60)
        print(f"Conductor: {DRIVER_ID}")
        print(f"Punto de recarga: {current_service['cp_id']}")
        print("-"*60)
        print(f"⚡ Potencia actual: {current_service['current_kw']:.1f} kW")
        print(f"🔋 Energía total: {current_service['total_kwh']:.2f} kWh")
        print(f"💶 Importe: €{current_service['total_euros']:.2f}")
        print(f"⏱️  Tiempo: {minutes:02d}:{seconds:02d}")
        print("="*60)
        input("\nPresiona ENTER para volver al menú...")

def wait_for_service_or_input():
    """Espera a que termine el servicio o usuario presione ENTER"""
    # TODO: Implementar espera con input no bloqueante
    input()

# ==================== MODO ARCHIVO ====================

def request_services_from_file():
    """Lee servicios de un archivo y los solicita secuencialmente"""
    filename = input("\nIntroduce el nombre del archivo (ej: services.txt): ").strip()
    
    if not filename:
        filename = "services.txt"
    
    try:
        with open(filename, 'r') as f:
            services = [line.strip() for line in f if line.strip()]
        
        if not services:
            print("El archivo está vacío")
            input("Presiona ENTER para continuar...")
            return
        
        print(f"\n📄 Cargados {len(services)} servicios del archivo")
        print("Iniciando proceso automático...\n")
        
        for idx, cp_id in enumerate(services, 1):
            print(f"\n[{idx}/{len(services)}] Procesando {cp_id}...")
            
            success, message = request_service(cp_id)
            
            if success:
                print(f"✅ Autorizado - Esperando completación...")
                wait_for_service_completion()
            else:
                print(f"❌ Denegado: {message}")
            
            # Esperar 4 segundos antes del siguiente
            if idx < len(services):
                print("⏳ Esperando 4 segundos antes del siguiente servicio...")
                time.sleep(4)
        
        print(f"\n✅ Todos los servicios procesados ({len(services)} total)")
        input("\nPresiona ENTER para continuar...")
        
    except FileNotFoundError:
        print(f"❌ Archivo '{filename}' no encontrado")
        input("Presiona ENTER para continuar...")
    except Exception as e:
        print(f"❌ Error leyendo archivo: {e}")
        input("Presiona ENTER para continuar...")

def wait_for_service_completion():
    """Espera a que el servicio actual complete"""
    timeout = 300  # 5 minutos máximo
    start = time.time()
    
    while True:
        with service_lock:
            if not current_service['active'] and current_service['total_kwh'] > 0:
                # Servicio completado
                return
        
        if time.time() - start > timeout:
            print("⚠️  Timeout esperando completación")
            return
        
        time.sleep(1)

def cancel_current_service():
    """Cancela el servicio actual si hay uno activo"""
    with service_lock:
        if current_service['active']:
            cp_id = current_service['cp_id']
            log_message(f'[DRIVER] Cancelando servicio en {cp_id}...')
            
            try:
                # Enviar cancelación a Central
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((CENTRAL_HOST, CENTRAL_PORT))
                
                cancel_msg = {
                    "type": "cancel_service",
                    "driver_id": DRIVER_ID,
                    "cp_id": cp_id
                }
                
                sock.sendall((json.dumps(cancel_msg) + "\n").encode('utf-8'))
                sock.close()
                
                log_message(f'[DRIVER] ✅ Servicio cancelado en {cp_id}')
            except Exception as e:
                log_message(f'[DRIVER] Error cancelando servicio: {e}')

# ==================== MAIN ====================

def main():
    """Función principal"""
    print("="*60)
    print("🚗 EV_Driver - Iniciando...")
    print("="*60)
    print(f"Driver ID: {DRIVER_ID}")
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Central: {CENTRAL_HOST}:{CENTRAL_PORT}")
    print("="*60)
    
    # Iniciar consumer de Kafka en thread separado
    kafka_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    kafka_thread.start()
    
    time.sleep(2)  # Dar tiempo a que Kafka conecte
    
    # Verificar argumentos para modo archivo
    if len(sys.argv) > 1 and '--services' in sys.argv:
        idx = sys.argv.index('--services')
        if idx + 1 < len(sys.argv):
            filename = sys.argv[idx + 1]
            print(f"\n📄 Modo automático con archivo: {filename}")
            request_services_from_file_auto(filename)
            return
    
    # Modo interactivo
    interactive_mode()

def request_services_from_file_auto(filename):
    """Versión automática sin interacción"""
    try:
        with open(filename, 'r') as f:
            services = [line.strip() for line in f if line.strip()]
        
        print(f"📄 Cargados {len(services)} servicios")
        
        for idx, cp_id in enumerate(services, 1):
            print(f"\n{'='*60}")
            print(f"[{idx}/{len(services)}] Servicio en {cp_id}")
            print(f"{'='*60}")
            
            success, message = request_service(cp_id)
            
            if success:
                wait_for_service_completion()
            else:
                print(f"Denegado: {message}")
            
            if idx < len(services):
                print("\n⏳ Esperando 4 segundos...")
                time.sleep(4)
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETADOS {len(services)} SERVICIOS")
        print(f"{'='*60}")
        
    except Exception as e:
        log_message(f"Error: {e}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log_message('\n[DRIVER] 🛑 Interrupt recibido...')
        cancel_current_service()
        log_message('[DRIVER] 👋 Saliendo...')
        sys.exit(0)