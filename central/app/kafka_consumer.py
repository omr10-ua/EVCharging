import os
import json
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from .data_manager import update_cp

def _make_bootstrap():
    host = os.environ.get("KAFKA_HOST", os.environ.get("KAFKA_BROKER", "kafka"))
    port = os.environ.get("KAFKA_PORT", "9092")
    return f"{host}:{port}"

class KafkaCentralConsumer:
    """
    Consumidor de Kafka que escucha telemetría de los CP Engines
    y actualiza el estado en Central
    """
    
    def __init__(self, socketio=None):
        self.socketio = socketio
        self.consumer = None
        self.running = False
        self._thread = None
        
        # Topics a consumir
        self.topic_telemetry = os.environ.get("KAFKA_TOPIC_TELEMETRY", "cp_telemetry")
        
        bs = _make_bootstrap()
        print(f"[KAFKA CONSUMER] Conectando a Kafka: {bs}")
        
        try:
            self.consumer = KafkaConsumer(
                self.topic_telemetry,
                bootstrap_servers=bs,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',  # Solo mensajes nuevos
                enable_auto_commit=True,
                group_id='central_consumer_group',
                consumer_timeout_ms=1000
            )
            print(f"[KAFKA CONSUMER] ✅ Conectado, escuchando topic: {self.topic_telemetry}")
        except Exception as e:
            print(f"[KAFKA CONSUMER] ❌ Error conectando: {e}")
            self.consumer = None
    
    def start(self):
        """Inicia el consumer en un thread separado"""
        if self.consumer is None:
            print("[KAFKA CONSUMER] ⚠️  Consumer no inicializado, no se puede iniciar")
            return
        
        self.running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        print("[KAFKA CONSUMER] 🚀 Thread iniciado")
    
    def stop(self):
        """Detiene el consumer"""
        self.running = False
        if self._thread:
            self._thread.join(timeout=2)
        if self.consumer:
            self.consumer.close()
        print("[KAFKA CONSUMER] 🛑 Detenido")
    
    def _consume_loop(self):
        """Loop principal que consume mensajes"""
        print("[KAFKA CONSUMER] 📡 Iniciando loop de consumo...")
        
        while self.running:
            try:
                # Poll por mensajes
                messages = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        self._process_message(record.value)
                        
            except KafkaError as e:
                print(f"[KAFKA CONSUMER] ❌ Error en poll: {e}")
            except Exception as e:
                print(f"[KAFKA CONSUMER] ❌ Error procesando mensajes: {e}")
    
    def _process_message(self, message):
        """Procesa un mensaje de telemetría"""
        try:
            msg_type = message.get("type")
            
            if msg_type == "telemetry":
                cp_id = message.get("cp_id")
                is_supplying = message.get("is_supplying", False)
                consumption_kw = message.get("consumption_kw", 0.0)
                total_kwh = message.get("total_kwh", 0.0)
                current_price = message.get("current_price", 0.0)
                driver_id = message.get("driver_id")
                
                # Actualizar estado en BD
                fields = {
                    "state": "SUMINISTRANDO" if is_supplying else "ACTIVADO",
                    "current_kw": float(consumption_kw),
                    "total_kwh": float(total_kwh),
                    "current_euros": float(total_kwh) * float(current_price)
                }
                
                if driver_id:
                    fields["current_driver"] = driver_id
                
                # Si no está suministrando, limpiar campos
                if not is_supplying:
                    fields["current_kw"] = 0.0
                    fields["total_kwh"] = 0.0
                    fields["current_euros"] = 0.0
                    fields["current_driver"] = None
                
                update_cp(cp_id, **fields)
                
                # Debug
                if is_supplying:
                    print(f"[KAFKA CONSUMER] 📊 {cp_id}: {consumption_kw:.1f} kW, {total_kwh:.2f} kWh, €{fields['current_euros']:.2f}")
                
            elif msg_type == "central_snapshot":
                # Ignorar snapshots propios
                pass
            
            else:
                # Otros tipos de mensaje
                pass
                
        except Exception as e:
            print(f"[KAFKA CONSUMER] ❌ Error procesando mensaje: {e}")
            import traceback
            traceback.print_exc()