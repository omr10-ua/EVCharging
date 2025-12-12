#!/usr/bin/env python3
"""
EV_Driver - Aplicación del conductor (MEJORADA)
Funciones:
- Solicita suministro en un CP específico
- Muestra telemetría en TIEMPO REAL durante el suministro
- Muestra ticket final al completar
- Puede leer peticiones desde un archivo
- Recibe updates vía Kafka de CENTRAL
"""

import os
import sys
import time
import json
import threading
import uuid
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Configuración
DRIVER_ID = os.getenv('DRIVER_ID', 'DRV001')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

class EVDriver:
    def __init__(self, driver_id, kafka_broker):
        self.driver_id = driver_id
        self.kafka_broker = kafka_broker
        self.producer = None
        self.consumer_updates = None  # Consumer para driver_updates
        self.consumer_telemetry = None  # Consumer para cp_telemetry
        self.running = True
        self.pending_requests = []
        self.current_cp = None
        self.current_price = 0.0
        self.supply_start_time = None
        
    def log(self, msg):
        """Logging con timestamp"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] [DRIVER-{self.driver_id}] {msg}')
    
    def init_kafka(self):
        """Inicializa Kafka"""
        try:
            self.log(f"Conectando a Kafka en {self.kafka_broker}...")
            
            # Producer para enviar peticiones
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            
            # Consumer para recibir respuestas de CENTRAL (autorizaciones/denegaciones)
            self.consumer_updates = KafkaConsumer(
                'driver_updates',
                bootstrap_servers=self.kafka_broker,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f'driver_{self.driver_id}_updates',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )
            
            # Consumer para recibir telemetría de CPs (suministro en tiempo real)
            # IMPORTANTE: Usamos group_id aleatorio para siempre leer mensajes recientes
            self.consumer_telemetry = KafkaConsumer(
                'cp_telemetry',
                bootstrap_servers=self.kafka_broker,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f'driver_{self.driver_id}_telemetry_{uuid.uuid4().hex[:8]}',
                auto_offset_reset='latest',
                enable_auto_commit=False,  # No guardar offset
                consumer_timeout_ms=1000
            )
            
            self.log("✓ Kafka inicializado")
            self.log("✓ Escuchando: driver_updates, cp_telemetry")
            return True
            
        except Exception as e:
            self.log(f"✗ Error inicializando Kafka: {e}")
            return False
    
    def request_charging(self, cp_id):
        """Solicita un suministro en un CP"""
        self.log(f"Solicitando suministro en CP {cp_id}...")
        
        request = {
            "type": "request_charging",
            "driver_id": self.driver_id,
            "cp_id": cp_id,
            "timestamp": time.time()
        }
        
        try:
            self.producer.send('driver_requests', request)
            self.producer.flush()
            self.log(f"✓ Petición enviada para CP {cp_id}")
            return True
        except Exception as e:
            self.log(f"✗ Error enviando petición: {e}")
            return False
    
    def consumer_updates_loop(self):
        """Loop que consume respuestas de autorización/denegación"""
        self.log("Iniciando consumer updates loop...")
        
        try:
            while self.running:
                messages = self.consumer_updates.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        if not self.running:
                            break
                        
                        try:
                            data = record.value
                            driver_id = data.get('driver_id')
                            
                            if driver_id != self.driver_id:
                                continue  # No es para nosotros
                            
                            payload = data.get('payload', {})
                            msg_type = payload.get('type')
                            
                            if msg_type == 'charging_authorized':
                                cp_id = payload.get('cp_id')
                                price = payload.get('price', 0.0)
                                self.current_cp = cp_id
                                self.current_price = price
                                self.supply_start_time = time.time()
                                
                                print()  # Nueva línea
                                self.log("="*60)
                                self.log("✅ SUMINISTRO AUTORIZADO")
                                self.log(f"   CP: {cp_id}")
                                self.log(f"   Precio: €{price:.2f}/kWh")
                                self.log("   🔌 Conecte su vehículo al punto de recarga")
                                self.log("="*60)
                                print()
                            
                            elif msg_type == 'charging_denied':
                                cp_id = payload.get('cp_id')
                                reason = payload.get('reason', 'Desconocido')
                                print()  # Nueva línea
                                self.log("="*60)
                                self.log("❌ SUMINISTRO DENEGADO")
                                self.log(f"   CP: {cp_id}")
                                self.log(f"   Razón: {reason}")
                                self.log("="*60)
                                print()
                                self.current_cp = None
                        
                        except Exception as e:
                            self.log(f"Error procesando mensaje updates: {e}")
        
        except Exception as e:
            self.log(f"Error en consumer updates loop: {e}")
            import traceback
            traceback.print_exc()
    
    def consumer_telemetry_loop(self):
        """Loop que consume telemetría en tiempo real"""
        self.log("Iniciando consumer telemetry loop...")
        
        last_print = 0
        
        try:
            while self.running:
                # Poll con timeout más corto para capturar mensajes más rápido
                messages = self.consumer_telemetry.poll(timeout_ms=500)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        if not self.running:
                            break
                        
                        try:
                            data = record.value
                            msg_type = data.get('type')
                            cp_id = data.get('cp_id')
                            driver_id = data.get('driver_id')
                            
                            # Solo procesar mensajes del CP donde estamos
                            if cp_id != self.current_cp:
                                continue
                            
                            # Solo procesar mensajes para nosotros (si tiene driver_id)
                            if driver_id and driver_id != self.driver_id:
                                continue
                            
                            if msg_type == 'supply_start':
                                print()  # Nueva línea
                                self.log("🔋 SUMINISTRO INICIADO - Esperando telemetría...")
                                print()
                            
                            elif msg_type == 'telemetry':
                                # Mostrar telemetría cada 2 segundos (no saturar pantalla)
                                now = time.time()
                                if now - last_print >= 2.0:
                                    is_supplying = data.get('is_supplying', False)
                                    if is_supplying:
                                        kw = data.get('consumption_kw', 0.0)
                                        kwh = data.get('total_kwh', 0.0)
                                        price = data.get('current_price', self.current_price)
                                        euros = kwh * price
                                        
                                        # Calcular tiempo transcurrido
                                        if self.supply_start_time:
                                            elapsed = int(now - self.supply_start_time)
                                            mins = elapsed // 60
                                            secs = elapsed % 60
                                            time_str = f"{mins:02d}:{secs:02d}"
                                        else:
                                            time_str = "00:00"
                                        
                                        print(f"\r⚡ {kw:5.1f} kW | 🔋 {kwh:6.3f} kWh | 💶 €{euros:6.2f} | ⏱️  {time_str}", end='', flush=True)
                                        last_print = now
                            
                            elif msg_type == 'supply_end':
                                # TICKET FINAL
                                total_kwh = data.get('total_kwh', 0.0)
                                total_euros = data.get('total_euros', 0.0)
                                reason = data.get('reason', 'completed')
                                
                                # Calcular duración
                                if self.supply_start_time:
                                    duration = int(time.time() - self.supply_start_time)
                                    mins = duration // 60
                                    secs = duration % 60
                                else:
                                    mins, secs = 0, 0
                                
                                # Imprimir ticket
                                print("\n\n")  # Dos nuevas líneas después de telemetría
                                print("="*60)
                                print("🧾 TICKET DE RECARGA")
                                print("="*60)
                                print(f"Conductor:        {self.driver_id}")
                                print(f"Punto de recarga: {cp_id}")
                                print(f"Fecha:            {time.strftime('%Y-%m-%d %H:%M:%S')}")
                                print("-"*60)
                                
                                if reason == 'completed':
                                    print("Estado:           ✅ COMPLETADO")
                                elif reason == 'driver_disconnected':
                                    print("Estado:           🛑 CANCELADO (Desconexión)")
                                elif reason == 'fault':
                                    print("Estado:           ⚠️  INTERRUMPIDO (Avería)")
                                elif reason == 'admin':
                                    print("Estado:           🛑 CANCELADO (Administrativo)")
                                else:
                                    print(f"Estado:           ⚠️  FINALIZADO ({reason})")
                                
                                print("-"*60)
                                print(f"⚡ Energía:         {total_kwh:.3f} kWh")
                                print(f"💶 Importe:         €{total_euros:.2f}")
                                print(f"⏱️  Duración:        {mins:02d}:{secs:02d}")
                                print("="*60)
                                print()
                                
                                # Resetear estado
                                self.current_cp = None
                                self.current_price = 0.0
                                self.supply_start_time = None
                        
                        except Exception as e:
                            self.log(f"Error procesando telemetría: {e}")
        
        except Exception as e:
            self.log(f"Error en consumer telemetry loop: {e}")
    
    def load_requests_from_file(self, filename):
        """Carga peticiones desde un archivo"""
        try:
            with open(filename, 'r') as f:
                lines = [line.strip() for line in f if line.strip()]
                self.pending_requests = lines
                self.log(f"✓ Cargadas {len(lines)} peticiones desde {filename}")
                return True
        except Exception as e:
            self.log(f"✗ Error cargando archivo: {e}")
            return False
    
    def process_requests_from_file(self):
        """Procesa las peticiones del archivo una a una"""
        self.log(f"Procesando {len(self.pending_requests)} peticiones...")
        
        for cp_id in self.pending_requests:
            if not self.running:
                break
            
            self.log(f"--- Petición para CP: {cp_id} ---")
            self.request_charging(cp_id)
            
            # Esperar 4 segundos antes de la siguiente
            for i in range(4, 0, -1):
                if not self.running:
                    break
                time.sleep(1)
        
        self.log("Todas las peticiones procesadas")
    
    def interactive_mode(self):
        """Modo interactivo por teclado"""
        print()
        self.log("="*60)
        self.log("MODO INTERACTIVO:")
        self.log("  r <CP_ID> = Solicitar suministro en CP")
        self.log("  f <archivo> = Cargar y procesar peticiones desde archivo")
        self.log("  s = Ver estado actual")
        self.log("  q = Salir")
        self.log("="*60)
        print()
        
        while self.running:
            try:
                # Mostrar prompt
                if self.current_cp:
                    prompt = f"{self.driver_id} [🔋 {self.current_cp}]> "
                else:
                    prompt = f"{self.driver_id}> "
                
                cmd = input(prompt).strip()
                
                if not cmd:
                    continue
                
                parts = cmd.split()
                action = parts[0].lower()
                
                if action == 'q':
                    self.log("Saliendo...")
                    self.running = False
                    break
                
                elif action == 'r' and len(parts) >= 2:
                    cp_id = parts[1]
                    self.request_charging(cp_id)
                
                elif action == 'f' and len(parts) >= 2:
                    filename = parts[1]
                    if self.load_requests_from_file(filename):
                        # Procesar en un hilo separado
                        t = threading.Thread(target=self.process_requests_from_file, daemon=True)
                        t.start()
                
                elif action == 's':
                    if self.current_cp:
                        self.log(f"📍 Suministrando en: {self.current_cp}")
                        self.log(f"💶 Precio: €{self.current_price:.2f}/kWh")
                    else:
                        self.log("Sin suministro activo")
                
                else:
                    self.log("Comando no reconocido")
                
            except EOFError:
                self.log("\nEOF detectado, saliendo...")
                self.running = False
                break
            except KeyboardInterrupt:
                self.log("\nInterrupción detectada, saliendo...")
                self.running = False
                break
            except Exception as e:
                self.log(f"Error en interactive mode: {e}")
    
    def run(self):
        """Ejecuta la aplicación"""
        if not self.init_kafka():
            self.log("Error inicializando Kafka, saliendo...")
            return
        
        # Iniciar consumer threads
        t1 = threading.Thread(target=self.consumer_updates_loop, daemon=True)
        t1.start()
        
        t2 = threading.Thread(target=self.consumer_telemetry_loop, daemon=True)
        t2.start()
        
        # Dar tiempo a que los consumers se inicien
        time.sleep(2)
        
        # Modo interactivo
        self.interactive_mode()
        
        # Cleanup
        self.log("Cerrando...")
        if self.consumer_updates:
            self.consumer_updates.close()
        if self.consumer_telemetry:
            self.consumer_telemetry.close()
        if self.producer:
            self.producer.close()

if __name__ == "__main__":
    driver = EVDriver(DRIVER_ID, KAFKA_BROKER)
    driver.run()