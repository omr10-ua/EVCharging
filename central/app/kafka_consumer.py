import os
import json
import threading
import time
import base64
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from .data_manager import get_cp, update_cp, load_data

def _make_bootstrap():
    host = os.environ.get("KAFKA_HOST", os.environ.get("KAFKA_BROKER", "kafka"))
    port = os.environ.get("KAFKA_PORT", "9092")
    return f"{host}:{port}"

def decrypt_message(encryption_key, encrypted_data):
    """
    Descifra un mensaje AES-256-CBC
    encrypted_data: base64(IV + ciphertext)
    """
    try:
        # Decodificar base64
        raw_data = base64.b64decode(encrypted_data)
        
        # Extraer IV (primeros 16 bytes) y ciphertext
        iv = raw_data[:16]
        ciphertext = raw_data[16:]
        
        # Decodificar la clave
        key = base64.b64decode(encryption_key)
        
        # Crear cipher
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        
        # Descifrar
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        
        # Quitar padding PKCS7
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()
        
        # Decodificar JSON
        return json.loads(plaintext.decode('utf-8'))
    
    except Exception as e:
        print(f"[DECRYPT] Error descifrando mensaje: {e}")
        return None

class KafkaCentralConsumer:
    """
    Consumidor de Kafka que escucha peticiones de drivers y 
    updates de telemetría de CPs
    """
    
    def __init__(self, producer=None, socketio=None, cp_server=None):
        bs = _make_bootstrap()
        self.producer = producer
        self.socketio = socketio
        self.cp_server = cp_server  # ✅ Referencia directa al socket server de CPs
        self._stop_flag = threading.Event()
        
        # Topics
        self.topic_driver_requests = os.environ.get("KAFKA_TOPIC_DRIVER_REQ", "driver_requests")
        self.topic_cp_telemetry = os.environ.get("KAFKA_TOPIC_CP_TELEM", "cp_telemetry")
        
        try:
            self.consumer = KafkaConsumer(
                self.topic_driver_requests,
                self.topic_cp_telemetry,
                bootstrap_servers=bs,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='central_consumer_group',
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            print(f"[KAFKA CONSUMER] Conectado a {bs}")
            print(f"[KAFKA CONSUMER] Escuchando topics: {self.topic_driver_requests}, {self.topic_cp_telemetry}")
        except Exception as e:
            print(f"[KAFKA CONSUMER] ERROR al inicializar: {e}")
            self.consumer = None
    
    def start(self):
        """Inicia el hilo del consumidor"""
        if self.consumer:
            t = threading.Thread(target=self._consume_loop, daemon=True)
            t.start()
            print("[KAFKA CONSUMER] Hilo iniciado")
    
    def stop(self):
        """Detiene el consumidor"""
        self._stop_flag.set()
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass
    
    def _consume_loop(self):
        """Loop principal que consume mensajes"""
        try:
            for message in self.consumer:
                if self._stop_flag.is_set():
                    break
                
                try:
                    self._process_message(message)
                except Exception as e:
                    print(f"[KAFKA CONSUMER] Error procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA CONSUMER] Error en consume loop: {e}")
        finally:
            print("[KAFKA CONSUMER] Loop finalizado")
    
    def _process_message(self, message):
        """Procesa un mensaje recibido"""
        topic = message.topic
        data = message.value
        
        # ✅ NUEVO: Descifrar mensajes cifrados de CPs
        if topic == self.topic_cp_telemetry and isinstance(data, dict):
            if data.get("encrypted") == True:
                cp_id = data.get("cp_id")
                encrypted_payload = data.get("data")
                
                if cp_id and encrypted_payload:
                    # Obtener encryption_key del CP
                    cp_data = load_data()
                    cp_info = cp_data.get("charging_points", {}).get(cp_id)
                    
                    if cp_info and "encryption_key" in cp_info:
                        encryption_key = cp_info["encryption_key"]
                        
                        # Descifrar
                        decrypted = decrypt_message(encryption_key, encrypted_payload)
                        
                        if decrypted:
                            print(f"[KAFKA CONSUMER] 🔓 Mensaje descifrado de {cp_id}")
                            data = decrypted  # Usar mensaje descifrado
                        else:
                            print(f"[KAFKA CONSUMER] ❌ No se pudo descifrar mensaje de {cp_id}")
                            return
                    else:
                        print(f"[KAFKA CONSUMER] ⚠️  No hay encryption_key para {cp_id}")
                        return
        
        # Procesar según topic
        if topic == self.topic_driver_requests:
            self._handle_driver_request(data)
        elif topic == self.topic_cp_telemetry:
            self._handle_cp_telemetry(data)
    
    def _handle_driver_request(self, data):
        """
        Maneja petición de suministro de un driver
        Formato esperado:
        {
            "type": "request_charging",
            "driver_id": "DRV001",
            "cp_id": "CP001"
        }
        """
        print(f"[KAFKA CONSUMER] 📞 Driver request: {data}")
        
        req_type = data.get("type")
        driver_id = data.get("driver_id")
        cp_id = data.get("cp_id")
        
        if req_type != "request_charging":
            return
        
        # Verificar estado del CP
        cp = get_cp(cp_id)
        
        if not cp:
            # CP no existe
            response = {
                "type": "charging_denied",
                "reason": "CP no existe",
                "cp_id": cp_id,
                "driver_id": driver_id
            }
            print(f"[CENTRAL] ❌ Petición DENEGADA: CP {cp_id} no existe")
            
        elif cp.get("state") not in ["ACTIVADO"]:
            # CP no disponible
            response = {
                "type": "charging_denied",
                "reason": f"CP no disponible (estado: {cp.get('state')})",
                "cp_id": cp_id,
                "driver_id": driver_id
            }
            print(f"[CENTRAL] ❌ Petición DENEGADA: CP {cp_id} estado {cp.get('state')}")
            
        elif cp.get("current_driver"):
            # CP ocupado
            response = {
                "type": "charging_denied",
                "reason": f"CP ocupado por {cp.get('current_driver')}",
                "cp_id": cp_id,
                "driver_id": driver_id
            }
            print(f"[CENTRAL] ❌ Petición DENEGADA: CP {cp_id} ocupado")
            
        else:
            # ✅ Autorizar
            update_cp(cp_id, state="SUMINISTRANDO", current_driver=driver_id)
            response = {
                "type": "charging_authorized",
                "cp_id": cp_id,
                "driver_id": driver_id,
                "price": cp.get("price", 0.0)
            }
            print(f"[CENTRAL] ✅ Petición AUTORIZADA: Driver {driver_id} -> CP {cp_id}")
            
            # ✅ CRÍTICO: Notificar al CP vía SOCKET para iniciar suministro
            if self.cp_server:
                command = {
                    "type": "start_supply",
                    "driver_id": driver_id
                }
                sent = self.cp_server.send_command_to_cp(cp_id, command)
                
                if sent:
                    print(f"[CENTRAL] ✅ Comando start_supply enviado a CP {cp_id}")
                else:
                    print(f"[CENTRAL] ⚠️  No se pudo enviar comando a CP {cp_id} (no conectado)")
            else:
                print(f"[CENTRAL] ⚠️  cp_server no disponible")
            
            # Notificar a interfaz web
            if self.socketio:
                self.socketio.emit('notification', {
                    'type': 'success',
                    'message': f'Servicio iniciado: {driver_id} en {cp_id}'
                }, namespace='/')
        
        # Enviar respuesta al driver vía Kafka
        if self.producer:
            self.producer.send_to_driver(driver_id, response)
    
    def _handle_cp_telemetry(self, data):
        """
        Maneja telemetría de CPs
        Formato esperado:
        {
            "type": "telemetry" | "supply_start" | "supply_end",
            "cp_id": "CP001",
            "is_supplying": true,
            "consumption_kw": 22.5,
            "total_kwh": 15.3,
            "driver_id": "DRV001",
            ...
        }
        """
        cp_id = data.get("cp_id")
        msg_type = data.get("type", "telemetry")
        
        if not cp_id:
            return
        
        # Obtener estado actual del CP
        current_cp = get_cp(cp_id)
        if not current_cp:
            print(f"[KAFKA CONSUMER] ⚠️  CP {cp_id} no encontrado")
            return
        
        current_state = current_cp.get("state")
        
        # ✅ CRÍTICO: NO sobrescribir estados administrativos
        if current_state in ["PARADO", "AVERIADO"]:
            print(f"[KAFKA CONSUMER] 🚫 Ignorando telemetría de {cp_id} (estado administrativo: {current_state})")
            return
        
        # ✅ CRÍTICO: Ignorar telemetría de CPs DESCONECTADOS
        if current_state == "DESCONECTADO":
            print(f"[KAFKA CONSUMER] 🚫 Ignorando telemetría de {cp_id} (DESCONECTADO)")
            return
        
        # Procesar según tipo de mensaje
        if msg_type == "supply_start":
            # Inicio de suministro
            driver_id = data.get("driver_id")
            print(f"[KAFKA CONSUMER] 🔋 CP {cp_id} inicia suministro para {driver_id}")
            update_cp(cp_id, 
                     state="SUMINISTRANDO", 
                     current_driver=driver_id,
                     current_kw=0.0,
                     total_kwh=0.0,
                     current_euros=0.0)
        
        elif msg_type == "supply_end":
            # Fin de suministro
            final_kwh = data.get("total_kwh", 0.0)
            final_euros = data.get("total_euros", 0.0)
            reason = data.get("reason", "completed")
            driver_id = data.get("driver_id")
            timestamp = data.get("timestamp", time.time())
            
            print(f"[KAFKA CONSUMER] ✅ CP {cp_id} finaliza suministro: {final_kwh:.2f} kWh, €{final_euros:.2f} ({reason})")
            
            # ✅ Guardar sesión en BD
            if driver_id and final_kwh > 0:
                from .data_manager import add_session
                session = {
                    "session_id": f"{cp_id}_{driver_id}_{int(timestamp)}",
                    "cp_id": cp_id,
                    "driver_id": driver_id,
                    "start_time": current_cp.get("last_update"),  # Aproximado
                    "end_time": datetime.fromtimestamp(timestamp).isoformat(),
                    "total_kwh": round(final_kwh, 3),
                    "total_euros": round(final_euros, 2),
                    "status": reason
                }
                add_session(session)
                print(f"[KAFKA CONSUMER] 💾 Sesión guardada: {session['session_id']}")
            
            # Si terminó por avería, marcar como AVERIADO
            if reason == "fault":
                update_cp(cp_id,
                         state="AVERIADO",
                         current_driver=None,
                         current_kw=0.0,
                         total_kwh=0.0,
                         current_euros=0.0)
            else:
                # Completado o cancelado: volver a ACTIVADO
                update_cp(cp_id,
                         state="ACTIVADO",
                         current_driver=None,
                         current_kw=0.0,
                         total_kwh=0.0,
                         current_euros=0.0)
        
        elif msg_type == "telemetry":
            # Telemetría en tiempo real
            is_supplying = data.get("is_supplying", False)
            
            # Actualizar datos de consumo
            fields = {}
            
            if "consumption_kw" in data:
                fields["current_kw"] = float(data["consumption_kw"] or 0.0)
            if "total_kwh" in data:
                fields["total_kwh"] = float(data["total_kwh"] or 0.0)
            if "current_price" in data:
                cp_price = float(data["current_price"] or 0.0)
            else:
                cp_price = current_cp.get("price", 0.35)
            
            # Calcular importe actual
            if "total_kwh" in fields:
                fields["current_euros"] = fields["total_kwh"] * cp_price
            
            # Actualizar estado solo si cambia entre ACTIVADO ↔ SUMINISTRANDO
            if is_supplying and current_state != "SUMINISTRANDO":
                fields["state"] = "SUMINISTRANDO"
            elif not is_supplying and current_state == "SUMINISTRANDO":
                # Si deja de suministrar, volver a ACTIVADO
                fields["state"] = "ACTIVADO"
                fields["current_driver"] = None
                fields["current_kw"] = 0.0
                fields["total_kwh"] = 0.0
                fields["current_euros"] = 0.0
            
            # Actualizar driver_id si viene en el mensaje
            if "driver_id" in data and data["driver_id"]:
                fields["current_driver"] = data["driver_id"]
            
            if fields:
                update_cp(cp_id, **fields)
                
                # Log solo si está suministrando
                if is_supplying and "current_kw" in fields:
                    kw = fields.get("current_kw", 0.0)
                    kwh = fields.get("total_kwh", 0.0)
                    euros = fields.get("current_euros", 0.0)
                    print(f"[KAFKA CONSUMER] 📊 CP {cp_id}: {kw:.1f} kW, {kwh:.2f} kWh, €{euros:.2f}")