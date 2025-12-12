#!/usr/bin/env python3
"""
EV_CP_E - Engine del Charging Point
Funciones:
- Servidor socket para recibir comandos del Monitor (health checks + start/stop supply)
- Producer de Kafka para enviar telemetría a Central
- Simulación de suministro eléctrico
"""

import socket
import threading
import time
import os
import sys
import json
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuración
CP_ID = os.getenv('CP_ID', 'CP001')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
MONITOR_PORT = int(os.getenv('MONITOR_PORT', 9101))
PRICE = float(os.getenv('CP_PRICE', 0.35))

class CPEngine:
    def __init__(self):
        self.cp_id = CP_ID
        self.kafka_broker = KAFKA_BROKER
        self.monitor_port = MONITOR_PORT
        self.price = PRICE
        
        # Estado del CP
        self.status = 'OK'  # OK or KO
        self.is_supplying = False
        self.current_driver = None
        self.consumption_kw = 0.0
        self.total_kwh = 0.0
        self.supply_start_time = None
        
        # ✅ Encryption key (recibida del Monitor)
        self.encryption_key = None
        
        # Control
        self.running = True
        self.server_sock = None
        
        # Kafka Producer (solo para telemetría)
        self.producer = None
        
    def log(self, msg):
        """Logging con timestamp"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] [ENGINE-{self.cp_id}] {msg}')
    
    def init_kafka(self):
        """Inicializa producer de Kafka para telemetría"""
        try:
            self.log(f"Conectando a Kafka en {self.kafka_broker}...")
            
            # Producer para telemetría
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                max_block_ms=5000
            )
            
            self.log("✓ Kafka Producer inicializado")
            return True
            
        except Exception as e:
            self.log(f"✗ Error inicializando Kafka: {e}")
            return False
    
    def start_monitor_server(self):
        """Inicia servidor socket para el Monitor"""
        def server_thread():
            self.log(f"Iniciando servidor Monitor en puerto {self.monitor_port}...")
            try:
                self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_sock.bind(('0.0.0.0', self.monitor_port))
                self.server_sock.listen(5)
                self.server_sock.settimeout(1.0)
                self.log(f"✓ Servidor Monitor escuchando en puerto {self.monitor_port}")
                
                while self.running:
                    try:
                        conn, addr = self.server_sock.accept()
                        threading.Thread(target=self.handle_monitor_conn, args=(conn,), daemon=True).start()
                    except socket.timeout:
                        continue
                    except Exception as e:
                        if self.running:
                            self.log(f"Error aceptando conexión: {e}")
                
            except Exception as e:
                self.log(f"✗ Error en servidor Monitor: {e}")
            finally:
                if self.server_sock:
                    self.server_sock.close()
                self.log("Servidor Monitor cerrado")
        
        t = threading.Thread(target=server_thread, daemon=True)
        t.start()
    
    def handle_monitor_conn(self, conn):
        """Maneja conexión del Monitor"""
        try:
            data = conn.recv(4096).decode().strip()
            
            # ✅ Health check simple
            if data == 'HEALTH_CHECK':
                conn.sendall(f'{self.status}\n'.encode())
                return
            
            # ✅ Comandos JSON
            try:
                command = json.loads(data)
                cmd_type = command.get('type')
                
                self.log(f"📨 Comando recibido: {cmd_type}")
                
                if cmd_type == 'start_supply':
                    driver_id = command.get('driver_id')
                    self.start_supply(driver_id)
                    conn.sendall(b'{"status":"ok"}\n')
                
                elif cmd_type == 'stop_supply':
                    reason = command.get('reason', 'admin')
                    self.stop_supply(reason)
                    conn.sendall(b'{"status":"ok"}\n')
                
                else:
                    conn.sendall(b'{"status":"unknown_command"}\n')
                    
            except json.JSONDecodeError:
                # No es JSON ni HEALTH_CHECK
                self.log(f"⚠ Mensaje desconocido: {data[:50]}")
                
        except Exception as e:
            self.log(f"Error en handle_monitor_conn: {e}")
        finally:
            conn.close()
    
    def start_supply(self, driver_id):
        """Inicia suministro"""
        if self.is_supplying:
            self.log(f"⚠ Ya estamos suministrando a {self.current_driver}")
            return
        
        self.log(f"🔌 INICIANDO SUMINISTRO para driver {driver_id}")
        self.is_supplying = True
        self.current_driver = driver_id
        self.consumption_kw = random.uniform(3.5, 22.0)  # kW aleatorio inicial
        self.total_kwh = 0.0
        self.supply_start_time = time.time()
        
        # Enviar mensaje de supply_start a Kafka
        start_msg = {
            "type": "supply_start",
            "cp_id": self.cp_id,
            "driver_id": driver_id,
            "timestamp": self.supply_start_time
        }
        
        # Enviar a Kafka (sin cifrar - Kafka es opcional)
        try:
            self.producer.send('cp_telemetry', start_msg)
            self.log(f"✓ Mensaje supply_start enviado a Kafka")
        except Exception as e:
            self.log(f"✗ Error enviando supply_start: {e}")
    
    def stop_supply(self, reason="completed"):
        """Detiene suministro"""
        if not self.is_supplying:
            return
        
        final_kwh = self.total_kwh
        final_euros = final_kwh * self.price
        driver_id = self.current_driver
        
        self.log(f"✓ FINALIZANDO SUMINISTRO ({reason}) - Total: {final_kwh:.2f} kWh - Importe: €{final_euros:.2f}")
        
        # Enviar mensaje de supply_end a Kafka
        end_msg = {
            "type": "supply_end",
            "cp_id": self.cp_id,
            "driver_id": driver_id,
            "total_kwh": round(final_kwh, 3),
            "total_euros": round(final_euros, 2),
            "reason": reason,
            "timestamp": time.time()
        }
        
        # Enviar a Kafka
        try:
            self.producer.send('cp_telemetry', end_msg)
            self.log(f"✓ Mensaje supply_end enviado a Kafka")
        except Exception as e:
            self.log(f"✗ Error enviando supply_end: {e}")
        
        self.is_supplying = False
        self.current_driver = None
        self.consumption_kw = 0.0
        self.total_kwh = 0.0
        self.supply_start_time = None
    
    def telemetry_loop(self):
        """Loop que envía telemetría cada segundo"""
        self.log("Iniciando telemetry loop...")
        
        while self.running:
            try:
                if self.is_supplying:
                    # Actualizar consumo
                    elapsed_hours = (time.time() - self.supply_start_time) / 3600.0
                    self.total_kwh = self.consumption_kw * elapsed_hours
                    
                    # Enviar telemetría
                    telemetry = {
                        "type": "telemetry",
                        "cp_id": self.cp_id,
                        "is_supplying": True,
                        "consumption_kw": round(self.consumption_kw, 2),
                        "total_kwh": round(self.total_kwh, 3),
                        "current_price": self.price,
                        "driver_id": self.current_driver
                    }
                    
                    self.producer.send('cp_telemetry', telemetry)
                    
                    # Log cada 5 segundos
                    if int(time.time()) % 5 == 0:
                        self.log(f"Suministrando: {self.consumption_kw:.2f} kW | Total: {self.total_kwh:.3f} kWh | {self.total_kwh * self.price:.2f} €")
                
                time.sleep(1)
                
            except Exception as e:
                self.log(f"Error en telemetry loop: {e}")
                time.sleep(1)
    
    def keyboard_listener(self):
        """Escucha comandos del teclado para simulación"""
        self.log("\n" + "="*60)
        self.log("CONTROLES:")
        self.log("  f = Simular avería (toggle)")
        self.log("  p = Iniciar suministro manual")
        self.log("  u = Detener suministro")
        self.log("  q = Salir")
        self.log("="*60)
        
        while self.running:
            try:
                cmd = input().strip().lower()
                
                if cmd == 'f':
                    self.status = 'KO' if self.status == 'OK' else 'OK'
                    self.log(f"Estado cambiado a: {self.status}")
                    if self.status == 'KO' and self.is_supplying:
                        self.stop_supply()
                
                elif cmd == 'p':
                    if not self.is_supplying:
                        # Simular inicio manual
                        driver_id = f"MANUAL_{int(time.time())}"
                        self.start_supply(driver_id)
                    else:
                        self.log("Ya hay un suministro activo")
                
                elif cmd == 'u':
                    self.stop_supply()
                
                elif cmd == 'q':
                    self.log("Saliendo...")
                    self.running = False
                    break
                
            except EOFError:
                break
            except Exception as e:
                self.log(f"Error en keyboard listener: {e}")
    
    def run(self):
        """Función principal"""
        self.log(f"=== Engine CP {self.cp_id} ===")
        self.log(f"Precio: {self.price} €/kWh")
        self.log(f"Kafka: {self.kafka_broker}")
        
        # Inicializar Kafka
        if not self.init_kafka():
            self.log("✗ No se pudo inicializar Kafka. Abortando.")
            return 1
        
        # Iniciar servidor Monitor
        self.start_monitor_server()
        
        # Iniciar telemetry thread
        telemetry_thread = threading.Thread(target=self.telemetry_loop, daemon=True)
        telemetry_thread.start()
        
        # Keyboard listener (blocking)
        try:
            self.keyboard_listener()
        except KeyboardInterrupt:
            self.log("\nCerrando...")
            self.running = False
        
        # Cleanup
        self.log("Limpiando recursos...")
        if self.producer:
            self.producer.flush()
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        
        return 0

def main():
    """Entry point"""
    if len(sys.argv) > 1:
        global CP_ID, KAFKA_BROKER, MONITOR_PORT, PRICE
        
        # Parsear: python ev_cp_e.py <kafka_broker> <monitor_port> <cp_id> [price]
        if len(sys.argv) >= 4:
            KAFKA_BROKER = sys.argv[1]
            MONITOR_PORT = int(sys.argv[2])
            CP_ID = sys.argv[3]
        if len(sys.argv) >= 5:
            PRICE = float(sys.argv[4])
    
    engine = CPEngine()
    return engine.run()

if __name__ == "__main__":
    sys.exit(main())