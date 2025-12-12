import socket
import threading
import json
import traceback
from .data_manager import ensure_cp_exists, update_cp, get_all_cps

class CPSocketServer:
    """
    Servidor TCP para conexiones de CP monitors.
    Protocolo: mensajes JSON separados por newline
    
    Mensajes esperados:
      - register: {"type":"register","cp_id":"CP001","location":"Madrid","price":0.35}
      - telemetry: {"type":"telemetry","cp_id":"CP001","is_supplying":true,"consumption_kw":22.5,"total_kwh":15.3}
      - fault: {"type":"fault","cp_id":"CP001","msg":"Hardware failure"}
      - disconnect: {"type":"disconnect","cp_id":"CP001"}
      - heartbeat: {"type":"heartbeat","cp_id":"CP001"}
    """

    def __init__(self, host="0.0.0.0", port=5001, producer=None, socketio=None):
        self.host = host
        self.port = port
        self.producer = producer
        self.socketio = socketio  # ✅ Para notificaciones al panel web
        self._sock = None
        self._clients = {}  # cp_id -> (conn,addr)
        self._stop_flag = threading.Event()
        self._client_threads = []

    def start(self):
        """Inicia el servidor en un hilo separado"""
        t = threading.Thread(target=self._serve_forever, daemon=True)
        t.start()

    def stop(self):
        """Detiene el servidor"""
        self._stop_flag.set()
        if self._sock:
            try:
                self._sock.close()
            except:
                pass

    def _serve_forever(self):
        """Loop principal del servidor"""
        print(f"[CP SOCKET] Servidor iniciado en {self.host}:{self.port}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            s.bind((self.host, self.port))
            s.listen(100)
            s.settimeout(1.0)  # timeout para poder chequear stop_flag
            self._sock = s
            
            while not self._stop_flag.is_set():
                try:
                    conn, addr = s.accept()
                    t = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                    t.start()
                    self._client_threads.append(t)
                except socket.timeout:
                    continue
                except Exception as e:
                    if not self._stop_flag.is_set():
                        print(f"[CP SOCKET] Error aceptando conexión: {e}")
        except Exception as e:
            print(f"[CP SOCKET] Error en server loop: {e}")
            traceback.print_exc()
        finally:
            s.close()
            print("[CP SOCKET] Servidor cerrado")

    def _handle_client(self, conn, addr):
        """Maneja la conexión de un cliente (CP Monitor)"""
        buff = b""
        cp_id = None
        
        try:
            print(f"[CP SOCKET] Nueva conexión desde {addr}")
            conn.settimeout(60.0)  # timeout de lectura
            
            # read loop
            while not self._stop_flag.is_set():
                try:
                    data = conn.recv(4096)
                    if not data:
                        print(f"[CP SOCKET] Conexión cerrada por cliente {addr}")
                        break
                    
                    buff += data
                    
                    # Procesar mensajes JSON separados por newline
                    while b'\n' in buff:
                        line, buff = buff.split(b'\n', 1)
                        line = line.strip()
                        
                        if not line:
                            continue
                        
                        try:
                            obj = json.loads(line.decode('utf-8'))
                            cp_id = self._process_message(obj, conn, addr, cp_id)
                        except json.JSONDecodeError as e:
                            print(f"[CP SOCKET] JSON decode error: {e} | data: {line[:100]}")
                        except Exception as e:
                            print(f"[CP SOCKET] Error procesando mensaje: {e}")
                            traceback.print_exc()
                
                except socket.timeout:
                    # No data received, continue
                    continue
                except ConnectionResetError:
                    print(f"[CP SOCKET] Conexión reset por peer {addr}")
                    break
                except Exception as e:
                    print(f"[CP SOCKET] Error en recv: {e}")
                    break

        except Exception as e:
            print(f"[CP SOCKET] Error en client handler: {e}")
            traceback.print_exc()
        finally:
            # Cleanup
            if cp_id:
                try:
                    update_cp(cp_id, state="DESCONECTADO", current_driver=None)
                    print(f"[CP SOCKET] CP {cp_id} desconectado")
                except:
                    pass
                if cp_id in self._clients:
                    del self._clients[cp_id]
            
            try:
                conn.close()
            except:
                pass
            
            print(f"[CP SOCKET] Conexión finalizada {addr}")

    def _process_message(self, obj, conn, addr, current_cp_id):
        """
        Procesa un mensaje JSON recibido.
        Retorna el cp_id actualizado
        """
        mtype = obj.get("type")
        cp_id = obj.get("cp_id", current_cp_id)
        
        if mtype == "authenticate":
            # ✅ NUEVO: Autenticación con credenciales del Registry
            username = obj.get("username")
            password = obj.get("password")
            location = obj.get("location", "Unknown")
            price = obj.get("price_eur_kwh", obj.get("price", 0.0))
            
            # Verificar credenciales
            from .data_manager import verify_cp_credentials, generate_encryption_key
            
            if not verify_cp_credentials(cp_id, username, password):
                print(f"[CP SOCKET] ✗ Autenticación DENEGADA: {cp_id} | credenciales inválidas")
                
                # Enviar respuesta de denegación
                nack = {
                    "type": "auth_ack",
                    "status": "denied",
                    "reason": "Invalid credentials"
                }
                self._send_to_client(conn, nack)
                return current_cp_id
            
            # Credenciales válidas - generar/obtener encryption_key
            encryption_key = generate_encryption_key(cp_id)
            
            # Actualizar CP en BD
            ensure_cp_exists(cp_id, location=location, price=price)
            update_cp(cp_id, state="ACTIVADO", location=location, price=float(price))
            
            # Guardar conexión
            self._clients[cp_id] = (conn, addr)
            
            print(f"[CP SOCKET] ✓ CP autenticado: {cp_id} | {location} | {addr}")
            
            # Enviar ACK con encryption_key
            ack = {
                "type": "auth_ack",
                "status": "ok",
                "cp_id": cp_id,
                "encryption_key": encryption_key
            }
            self._send_to_client(conn, ack)
            
            # Notificar al panel web
            if self.socketio:
                self.socketio.emit('notification', {
                    'type': 'success',
                    'message': f'CP {cp_id} autenticado y ACTIVADO'
                }, namespace='/')
            
            return cp_id
        
        elif mtype == "register":
            # Registro antiguo (mantenido por compatibilidad)
            location = obj.get("location", "Unknown")
            price = obj.get("price_eur_kwh", obj.get("price", 0.0))
            
            ensure_cp_exists(cp_id, location=location, price=price)
            update_cp(cp_id, state="ACTIVADO", location=location, price=float(price))
            
            print(f"[CP SOCKET] ✓ CP registrado: {cp_id} | {location} | {price}€/kWh | {addr}")
            
            # Guardar conexión
            self._clients[cp_id] = (conn, addr)
            
            # Enviar ACK
            ack = {"type":"register_ack","status":"ok", "cp_id": cp_id}
            self._send_to_client(conn, ack)
            
            return cp_id
        
        elif mtype == "telemetry":
            # Telemetría del CP
            if cp_id:
                fields = {}
                
                if "is_supplying" in obj:
                    fields["state"] = "SUMINISTRANDO" if obj.get("is_supplying") else "ACTIVADO"
                
                if "consumption_kw" in obj:
                    fields["current_kw"] = float(obj.get("consumption_kw") or 0.0)
                
                if "total_kwh" in obj:
                    fields["total_kwh"] = float(obj.get("total_kwh") or 0.0)
                
                if "current_driver" in obj:
                    fields["current_driver"] = obj.get("current_driver")
                
                # Calcular importe si tenemos los datos
                cp_data = ensure_cp_exists(cp_id)
                price = cp_data.get("price", 0.0)
                if "total_kwh" in fields:
                    fields["current_euros"] = fields["total_kwh"] * price
                
                update_cp(cp_id, **fields)
                
                # Publish monitor snapshot if producer exists
                if self.producer:
                    snapshot = {"type":"telemetry","cp_id":cp_id,"payload":obj}
                    self.producer.publish_monitor(snapshot)
        
        elif mtype == "fault":
            # Avería reportada
            if cp_id:
                update_cp(cp_id, state="AVERIADO", current_driver=None)
                print(f"[CP SOCKET] ⚠ AVERÍA en CP {cp_id}: {obj.get('msg', 'sin detalles')}")
                
                if self.producer:
                    self.producer.publish_monitor({
                        "type":"fault",
                        "cp_id":cp_id,
                        "msg":obj.get("msg")
                    })
        
        elif mtype == "disconnect":
            # Desconexión explícita
            if cp_id:
                update_cp(cp_id, state="DESCONECTADO", current_driver=None)
                print(f"[CP SOCKET] CP {cp_id} solicitó desconexión")
            return None  # Signal to close connection
        
        elif mtype == "heartbeat":
            # Heartbeat - mantener vivo
            if cp_id:
                # Enviar ACK
                self._send_to_client(conn, {"type":"heartbeat_ack"})
        
        else:
            print(f"[CP SOCKET] Mensaje desconocido: {mtype}")
        
        return cp_id

    def _send_to_client(self, conn, obj):
        """Envía un mensaje JSON al cliente"""
        try:
            msg = json.dumps(obj) + "\n"
            conn.sendall(msg.encode('utf-8'))
        except Exception as e:
            print(f"[CP SOCKET] Error enviando a cliente: {e}")

    def send_command_to_cp(self, cp_id, command):
        """
        Envía un comando a un CP específico
        command: dict - puede ser start_supply, stop_supply, o comando administrativo
        Ejemplos:
          - {"type": "start_supply", "driver_id": "DRV001"}
          - {"type": "stop_supply", "reason": "admin"}
          - {"type": "command", "action": "stop"}
        """
        if cp_id not in self._clients:
            print(f"[CP SOCKET] ⚠️  CP {cp_id} no conectado")
            return False
        
        conn, addr = self._clients[cp_id]
        try:
            # Enviar el comando directamente (ya tiene el formato correcto)
            self._send_to_client(conn, command)
            print(f"[CP SOCKET] ✅ Comando enviado a {cp_id}: {command.get('type', 'unknown')}")
            return True
        except Exception as e:
            print(f"[CP SOCKET] ❌ Error enviando comando a {cp_id}: {e}")
            traceback.print_exc()
            return False