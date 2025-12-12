#!/usr/bin/env python3
"""
EV_CP_M - Monitor del Charging Point
Funciones:
- Se conecta a CENTRAL vía socket para autenticación y registro
- Monitoriza la salud del Engine cada segundo
- Reporta averías a CENTRAL
"""

import socket
import time
import os
import sys
import json
import threading
import requests  # Para llamadas API al Registry

# Configuración desde variables de entorno o argumentos
CP_ID = os.getenv('CP_ID', 'CP001')
REGISTRY_URL = os.getenv('REGISTRY_URL', 'http://localhost:8000/api/registry')
CENTRAL_HOST = os.getenv('CENTRAL_HOST', 'localhost')
CENTRAL_PORT = int(os.getenv('CENTRAL_PORT', 5001))
ENGINE_MONITOR_PORT = int(os.getenv('ENGINE_MONITOR_PORT', 6001))
LOCATION = os.getenv('CP_LOCATION', 'Unknown Location')
PRICE = float(os.getenv('CP_PRICE', 0.35))

class CPMonitor:
    def __init__(self):
        self.cp_id = CP_ID
        self.central_sock = None
        self.connected = False
        self.running = True
        
        # Credenciales del Registry
        self.username = None
        self.password = None
        
        # Clave de cifrado de Central
        self.encryption_key = None
        
    def log(self, msg):
        """Logging con timestamp"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] [MONITOR-{self.cp_id}] {msg}')
    
    def register_in_registry(self):
        """
        Paso 1: Registrarse en Registry para obtener credenciales
        Registry debe devolver username/password para autenticación en Central
        """
        try:
            self.log(f"📝 Registrando en Registry: {REGISTRY_URL}")
            
            # Payload para registro
            payload = {
                "cp_id": self.cp_id,
                "location": LOCATION,
                "price": PRICE,
                "force": True  # ✅ Forzar re-registro si ya existe
            }
            
            # POST al Registry (debería ser HTTPS en producción)
            response = requests.post(
                f"{REGISTRY_URL}/register",
                json=payload,
                timeout=5
            )
            
            # Aceptar 200 OK o 201 Created
            if response.status_code in [200, 201]:
                data = response.json()
                self.username = data.get('credentials', {}).get('username') or data.get('username')
                self.password = data.get('credentials', {}).get('password') or data.get('password')
                
                if self.username and self.password:
                    self.log(f"✓ Credenciales obtenidas del Registry")
                    self.log(f"  Username: {self.username}")
                    self.log(f"  Password: {'*' * len(self.password)}")
                    return True
                else:
                    self.log(f"✗ Registry no devolvió credenciales")
                    return False
            else:
                # Mostrar respuesta del servidor para debug
                try:
                    error_data = response.json()
                    self.log(f"✗ Error en Registry: HTTP {response.status_code}")
                    self.log(f"  Respuesta: {error_data}")
                except:
                    self.log(f"✗ Error en Registry: HTTP {response.status_code}")
                    self.log(f"  Respuesta: {response.text[:200]}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.log(f"✗ No se pudo conectar con Registry en: {REGISTRY_URL}")
            return False
        except Exception as e:
            self.log(f"✗ Error registrando en Registry: {e}")
            return False
    
    def connect_to_central(self):
        """
        Paso 2: Conecta y se autentica en CENTRAL usando credenciales del Registry
        Central debe devolver encryption_key para cifrado de mensajes Kafka
        """
        try:
            self.log(f"🔌 Conectando a CENTRAL en {CENTRAL_HOST}:{CENTRAL_PORT}...")
            self.central_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_sock.connect((CENTRAL_HOST, CENTRAL_PORT))
            self.central_sock.settimeout(5.0)
            
            # Enviar mensaje de autenticación con credenciales
            auth_msg = {
                "type": "authenticate",
                "cp_id": self.cp_id,
                "username": self.username,
                "password": self.password,
                "location": LOCATION,
                "price_eur_kwh": PRICE
            }
            
            self.send_to_central(auth_msg)
            self.log(f"🔐 Autenticando en CENTRAL con credenciales...")
            
            # Esperar respuesta de autenticación
            response = self.receive_from_central()
            if response and response.get("type") == "auth_ack":
                if response.get("status") == "ok":
                    # Recibir encryption_key
                    self.encryption_key = response.get("encryption_key")
                    
                    if self.encryption_key:
                        self.connected = True
                        self.log(f"✓ AUTENTICADO en CENTRAL exitosamente")
                        self.log(f"✓ Encryption key recibida: {self.encryption_key[:8]}...")
                        
                        # Enviar encryption_key al Engine
                        self.send_encryption_key_to_engine()
                        return True
                    else:
                        self.log(f"✗ No se recibió encryption_key")
                        return False
                else:
                    reason = response.get("reason", "unknown")
                    self.log(f"✗ Autenticación DENEGADA: {reason}")
                    return False
            else:
                self.log(f"✗ No se recibió AUTH_ACK correcto: {response}")
                return False
                
        except Exception as e:
            self.log(f"✗ ERROR conectando a CENTRAL: {e}")
            return False
    
    def send_to_central(self, obj):
        """Envía mensaje JSON a CENTRAL"""
        try:
            msg = json.dumps(obj) + "\n"
            self.central_sock.sendall(msg.encode('utf-8'))
        except Exception as e:
            self.log(f"Error enviando a CENTRAL: {e}")
            self.connected = False
    
    def receive_from_central(self, timeout=3):
        """Recibe mensaje de CENTRAL"""
        try:
            self.central_sock.settimeout(timeout)
            data = b''
            while b'\n' not in data:
                chunk = self.central_sock.recv(1024)
                if not chunk:
                    return None
                data += chunk
            
            return json.loads(data.decode('utf-8').strip())
        except socket.timeout:
            return None
        except Exception as e:
            self.log(f"Error recibiendo de CENTRAL: {e}")
            return None
    
    def check_engine_health(self):
        """Verifica la salud del Engine"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(('localhost', ENGINE_MONITOR_PORT))
            
            # Enviar HEALTH_CHECK
            sock.sendall(b'HEALTH_CHECK\n')
            
            # Recibir respuesta
            response = sock.recv(1024).decode().strip()
            sock.close()
            
            return response == 'OK'
            
        except Exception as e:
            # Engine no responde o error
            return False
    
    def send_command_to_engine(self, command):
        """Envía un comando al Engine (puerto 9101)"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect(('localhost', ENGINE_MONITOR_PORT))
            
            # Enviar comando como JSON
            msg = json.dumps(command) + '\n'
            sock.sendall(msg.encode('utf-8'))
            
            self.log(f"✓ Comando enviado a Engine: {command['type']}")
            sock.close()
            return True
            
        except Exception as e:
            self.log(f"✗ Error enviando comando a Engine: {e}")
            return False
    
    def send_encryption_key_to_engine(self):
        """
        Paso 3: Informa al Engine que se recibió encryption_key
        NOTA: No ciframos Kafka (es opcional), solo guardamos la key por si se implementa después
        """
        try:
            self.log(f"🔑 Encryption key recibida y almacenada: {self.encryption_key[:8]}...")
            self.log(f"ℹ️  (Cifrado Kafka no implementado - es opcional)")
            
            # Opcional: podrías enviar la key al Engine si quisieras cifrar después
            # Por ahora, solo la guardamos en Monitor
            
            return True
                
        except Exception as e:
            self.log(f"✗ Error con encryption_key: {e}")
            return False
    
    def listen_central_commands(self):
        """Thread que escucha comandos de Central"""
        self.log("Thread de comandos CENTRAL iniciado")
        
        buffer = b''  # Buffer acumulativo
        
        while self.running:
            try:
                # Intentar recibir comando de Central (timeout corto)
                if self.central_sock:
                    self.central_sock.settimeout(0.5)
                    try:
                        # Recibir chunk
                        chunk = self.central_sock.recv(4096)
                        if not chunk:
                            # Conexión cerrada
                            self.log("⚠ Conexión con CENTRAL cerrada")
                            self.running = False
                            break
                        
                        buffer += chunk
                        
                        # Procesar todos los mensajes completos en el buffer
                        while b'\n' in buffer:
                            line, buffer = buffer.split(b'\n', 1)
                            
                            if line:
                                try:
                                    msg = json.loads(line.decode('utf-8'))
                                    msg_type = msg.get("type")
                                    
                                    self.log(f"📨 Comando recibido de CENTRAL: {msg_type}")
                                    
                                    if msg_type == "start_supply":
                                        # Reenviar al Engine
                                        driver_id = msg.get("driver_id")
                                        self.log(f"🚗 START_SUPPLY para driver {driver_id}")
                                        
                                        engine_cmd = {
                                            "type": "start_supply",
                                            "driver_id": driver_id,
                                            "cp_id": self.cp_id
                                        }
                                        self.send_command_to_engine(engine_cmd)
                                    
                                    elif msg_type == "stop_supply":
                                        # Reenviar al Engine
                                        reason = msg.get("reason", "admin")
                                        self.log(f"🛑 STOP_SUPPLY (razón: {reason})")
                                        
                                        engine_cmd = {
                                            "type": "stop_supply",
                                            "reason": reason
                                        }
                                        self.send_command_to_engine(engine_cmd)
                                    
                                    elif msg_type == "command":
                                        # Comandos administrativos (parar/reanudar)
                                        action = msg.get("action")
                                        self.log(f"⚙️ Comando administrativo: {action}")
                                        
                                        if action == "stop":
                                            # Marcar CP como PARADO
                                            status_msg = {
                                                "type": "status_change",
                                                "cp_id": self.cp_id,
                                                "state": "PARADO"
                                            }
                                            self.send_to_central(status_msg)
                                        
                                        elif action == "resume":
                                            # Marcar CP como ACTIVADO
                                            status_msg = {
                                                "type": "status_change",
                                                "cp_id": self.cp_id,
                                                "state": "ACTIVADO"
                                            }
                                            self.send_to_central(status_msg)
                                
                                except json.JSONDecodeError as e:
                                    self.log(f"✗ Error parseando comando: {e}")
                    
                    except socket.timeout:
                        # No hay datos, continuar
                        pass
                
            except Exception as e:
                self.log(f"Error en listen_central_commands: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)
        
        self.log("Thread de comandos CENTRAL finalizado")
    
    def monitor_loop(self):
        """Loop principal de monitorización"""
        self.log("Iniciando loop de monitorización...")
        
        fault_count = 0
        was_faulty = False
        
        while self.running:
            try:
                # Verificar salud del engine
                is_healthy = self.check_engine_health()
                
                if not is_healthy:
                    fault_count += 1
                    if fault_count >= 2 and not was_faulty:
                        # Reportar avería a CENTRAL
                        fault_msg = {
                            "type": "fault",
                            "cp_id": self.cp_id,
                            "msg": "Engine no responde a health checks"
                        }
                        self.send_to_central(fault_msg)
                        self.log("⚠ AVERÍA detectada - Notificado a CENTRAL")
                        was_faulty = True
                else:
                    # Engine está OK
                    if was_faulty:
                        # Se recuperó de la avería
                        self.log("✓ Engine recuperado")
                        # Enviar telemetría normal para actualizar estado
                        telemetry = {
                            "type": "telemetry",
                            "cp_id": self.cp_id,
                            "is_supplying": False
                        }
                        self.send_to_central(telemetry)
                        was_faulty = False
                    
                    fault_count = 0
                
                # Enviar heartbeat cada 5 segundos
                if int(time.time()) % 5 == 0:
                    heartbeat = {
                        "type": "heartbeat",
                        "cp_id": self.cp_id
                    }
                    self.send_to_central(heartbeat)
                
                time.sleep(1)
                
            except Exception as e:
                self.log(f"Error en monitor loop: {e}")
                time.sleep(1)
    
    def run(self):
        """Función principal"""
        self.log(f"=== Monitor CP {self.cp_id} ===")
        self.log(f"Ubicación: {LOCATION}")
        self.log(f"Precio: {PRICE} €/kWh")
        self.log(f"Engine Monitor Port: {ENGINE_MONITOR_PORT}")
        self.log(f"Registry URL: {REGISTRY_URL}")
        
        # ✅ PASO 1: Registrarse en Registry para obtener credenciales
        if not self.register_in_registry():
            self.log("✗ No se pudo registrar en Registry. Abortando.")
            return 1
        
        # ✅ PASO 2: Autenticarse en CENTRAL con credenciales
        if not self.connect_to_central():
            self.log("✗ No se pudo autenticar en CENTRAL. Abortando.")
            return 1
        
        # ✅ Iniciar thread para escuchar comandos de Central
        import threading
        commands_thread = threading.Thread(target=self.listen_central_commands, daemon=True)
        commands_thread.start()
        self.log("✓ Thread de comandos CENTRAL iniciado")
        
        # Iniciar monitorización
        try:
            self.monitor_loop()
        except KeyboardInterrupt:
            self.log("\nDeteniendo monitor...")
            self.running = False
        
        # Desconectar
        if self.connected:
            try:
                disconnect_msg = {
                    "type": "disconnect",
                    "cp_id": self.cp_id
                }
                self.send_to_central(disconnect_msg)
                self.central_sock.close()
                self.log("Desconectado de CENTRAL")
            except:
                pass
        
        return 0

def main():
    """Entry point"""
    if len(sys.argv) > 1:
        global CP_ID, CENTRAL_HOST, CENTRAL_PORT, LOCATION, PRICE
        
        # Parsear argumentos: python ev_cp_m.py <central_host> <central_port> <cp_id> [location] [price]
        if len(sys.argv) >= 4:
            CENTRAL_HOST = sys.argv[1]
            CENTRAL_PORT = int(sys.argv[2])
            CP_ID = sys.argv[3]
        if len(sys.argv) >= 5:
            LOCATION = sys.argv[4]
        if len(sys.argv) >= 6:
            PRICE = float(sys.argv[5])
    
    monitor = CPMonitor()
    return monitor.run()

if __name__ == "__main__":
    sys.exit(main())