import socket
import threading
import json
import traceback
from .data_manager import ensure_cp_exists, update_cp, get_all_cps, load_data

class CPSocketServer:
    """
    Servidor TCP que acepta conexiones de CP monitors.
    Protocolo: mensajes JSON por conexión.
    
    Mensajes esperados:
      - register: {"type":"register","cp_id":"CP001","location":"...","price":0.35}
      - telemetry: {"type":"telemetry","cp_id":"CP001",...}
      - fault: {"type":"fault","cp_id":"CP001","msg":"..."}
      - disconnect: {"type":"disconnect","cp_id":"CP001"}
    """

    def __init__(self, host="0.0.0.0", port=5001, producer=None, socketio=None):
        self.host = host
        self.port = port
        self.producer = producer
        self.socketio = socketio
        self._sock = None
        self._clients = {}  # cp_id -> (conn,addr)
        self._stop_flag = threading.Event()
        self._clients_lock = threading.Lock()  # ✅ Lock para thread-safe

    def start(self):
        t = threading.Thread(target=self._serve_forever, daemon=True)
        t.start()

    def stop(self):
        self._stop_flag.set()
        if self._sock:
            try:
                self._sock.close()
            except:
                pass

    def _notify_web(self, message, msg_type='info'):
        """Envía notificación al panel web vía WebSocket"""
        if self.socketio:
            try:
                self.socketio.emit('notification', {
                    'type': msg_type,
                    'message': message
                }, namespace='/')
            except Exception as e:
                print(f"[CP SOCKET] Error enviando notificación web: {e}")

    # ✅ NUEVO: Método para enviar comandos a un CP específico
    def send_command_to_cp(self, cp_id, command):
        """
        Envía un comando a un CP específico.
        command = {"type": "command", "action": "stop"} o {"action": "resume"}
        Returns: True si se envió correctamente, False si no
        """
        with self._clients_lock:
            if cp_id not in self._clients:
                print(f"[CP SOCKET] ⚠️  CP {cp_id} no está conectado, no se puede enviar comando")
                return False
            
            conn, addr = self._clients[cp_id]
        
        try:
            msg = json.dumps(command) + "\n"
            conn.sendall(msg.encode('utf-8'))
            print(f"[CP SOCKET] ✅ Comando enviado a {cp_id}: {command}")
            return True
        except Exception as e:
            print(f"[CP SOCKET] ❌ Error enviando comando a {cp_id}: {e}")
            # Si falla, quitar de la lista de clientes
            with self._clients_lock:
                if cp_id in self._clients:
                    del self._clients[cp_id]
            return False

    def _serve_forever(self):
        print(f"[CP SOCKET] 🔌 Iniciando servidor en {self.host}:{self.port}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(100)
        self._sock = s
        try:
            while not self._stop_flag.is_set():
                conn, addr = s.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print("[CP SOCKET] Error en server loop:", e)
        finally:
            s.close()

    def _handle_client(self, conn, addr):
        buff = b""
        cp_id = None
        try:
            # read loop
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buff += data
                
                # try to decode full JSONs possibly concatenated
                while True:
                    try:
                        text = buff.decode("utf-8").strip()
                    except UnicodeDecodeError:
                        # wait for more data
                        break
                    if not text:
                        buff = b""
                        break
                    
                    # attempt to parse first JSON object
                    try:
                        # allow multiple JSON per buffer if newline separated
                        if "\n" in text:
                            first, rest = text.split("\n", 1)
                            obj = json.loads(first.strip())
                            buff = rest.encode("utf-8")
                        else:
                            obj = json.loads(text)
                            buff = b""
                    except json.JSONDecodeError:
                        # incomplete JSON -> wait more bytes
                        break

                    # ============ PROCESAR MENSAJE ============
                    mtype = obj.get("type")
                    
                    if mtype == "register":
                        cp_id = obj.get("cp_id")
                        location = obj.get("location", "Unknown")
                        price = obj.get("price_eur_kwh", obj.get("price", 0.0))
                        
                        ensure_cp_exists(cp_id, location=location, price=price)
                        update_cp(cp_id, state="ACTIVADO", location=location, price=float(price))
                        
                        print(f"[CP SOCKET] ✅ CP registrado: {cp_id} @ {location} desde {addr}")
                        
                        # Guardar cliente
                        with self._clients_lock:
                            self._clients[cp_id] = (conn, addr)
                        
                        # Notificar al panel web
                        self._notify_web(f"CP {cp_id} registrado y ACTIVADO", 'success')
                        
                        # Enviar ACK
                        ack = {"type": "register_ack", "status": "ok"}
                        try:
                            conn.sendall((json.dumps(ack) + "\n").encode("utf-8"))
                        except:
                            pass
                    
                    elif mtype == "telemetry":
                        cp_id = obj.get("cp_id")
                        if cp_id:
                            # Actualizar campos
                            fields = {}
                            if "is_supplying" in obj:
                                is_supplying = obj.get("is_supplying")
                                fields["state"] = "SUMINISTRANDO" if is_supplying else "ACTIVADO"
                            
                            if "consumption_kw" in obj:
                                fields["current_kw"] = float(obj.get("consumption_kw") or 0.0)
                            
                            if "total_kwh" in obj:
                                total_kwh = float(obj.get("total_kwh") or 0.0)
                                fields["total_kwh"] = total_kwh
                                
                                # Calcular importe en euros
                                if "current_price" in obj:
                                    price = float(obj.get("current_price") or 0.0)
                                    fields["current_euros"] = total_kwh * price
                            
                            if "driver_id" in obj:
                                fields["current_driver"] = obj.get("driver_id")
                            
                            update_cp(cp_id, **fields)
                            
                            # Publicar a Kafka si hay producer
                            if self.producer:
                                snapshot = {"type": "telemetry", "cp_id": cp_id, "payload": obj}
                                self.producer.publish_monitor(snapshot)
                    
                    elif mtype == "fault":
                        cp_id = obj.get("cp_id")
                        fault_msg = obj.get("msg", "Unknown fault")
                        
                        update_cp(cp_id, state="AVERIADO")
                        
                        print(f"[CP SOCKET] 🔴 AVERÍA en {cp_id}: {fault_msg}")
                        
                        # Notificar al panel web
                        self._notify_web(f"⚠️ AVERÍA detectada en CP {cp_id}", 'error')
                        
                        if self.producer:
                            self.producer.publish_monitor({
                                "type": "fault",
                                "cp_id": cp_id,
                                "msg": fault_msg
                            })
                    
                    elif mtype == "disconnect":
                        cp_id = obj.get("cp_id")
                        update_cp(cp_id, state="DESCONECTADO")
                        
                        print(f"[CP SOCKET] ⚫ CP {cp_id} desconectado")
                        
                        # Notificar al panel web
                        self._notify_web(f"CP {cp_id} desconectado", 'info')
                        
                        break
                    
                    # ✅ NUEVO: Respuesta a comando (ACK del CP)
                    elif mtype == "command_ack":
                        cp_id = obj.get("cp_id")
                        action = obj.get("action")
                        status = obj.get("status")
                        print(f"[CP SOCKET] 📨 ACK de {cp_id}: {action} -> {status}")
                    
                    # ✅ NUEVO: Cambio de estado confirmado por CP
                    elif mtype == "status_change":
                        cp_id = obj.get("cp_id")
                        new_state = obj.get("state")
                        update_cp(cp_id, state=new_state)
                        print(f"[CP SOCKET] 🔄 Estado confirmado de {cp_id}: {new_state}")
                    
                    # ✅ NUEVO: Solicitud de servicio de un Driver
                    elif mtype == "service_request":
                        driver_id = obj.get("driver_id")
                        requested_cp_id = obj.get("cp_id")
                        
                        print(f"[CP SOCKET] 📞 Solicitud de servicio: Driver {driver_id} → CP {requested_cp_id}")
                        
                        # Validar que el CP existe y está disponible
                        from .data_manager import get_cp
                        target_cp = get_cp(requested_cp_id)
                        
                        response = {"type": "service_response"}
                        
                        if not target_cp:
                            response["status"] = "denied"
                            response["reason"] = "CP no existe"
                            print(f"[CP SOCKET] ❌ CP {requested_cp_id} no existe")
                        
                        elif target_cp.get("state") != "ACTIVADO":
                            response["status"] = "denied"
                            response["reason"] = f"CP en estado {target_cp.get('state')}"
                            print(f"[CP SOCKET] ❌ CP {requested_cp_id} no disponible: {target_cp.get('state')}")
                        
                        elif target_cp.get("current_driver"):
                            response["status"] = "denied"
                            response["reason"] = f"CP ocupado por {target_cp.get('current_driver')}"
                            print(f"[CP SOCKET] ❌ CP {requested_cp_id} ocupado")
                        
                        else:
                            # Autorizar servicio
                            response["status"] = "authorized"
                            response["cp_id"] = requested_cp_id
                            
                            # Actualizar BD con el driver
                            update_cp(requested_cp_id, 
                                    state="SUMINISTRANDO",
                                    current_driver=driver_id)
                            
                            # Enviar comando al CP para iniciar suministro
                            start_command = {
                                "type": "start_supply",
                                "driver_id": driver_id
                            }
                            self.send_command_to_cp(requested_cp_id, start_command)
                            
                            print(f"[CP SOCKET] ✅ Servicio autorizado: {driver_id} en {requested_cp_id}")
                            
                            # Notificar al panel web
                            self._notify_web(f"Servicio iniciado: {driver_id} en {requested_cp_id}", 'success')
                        
                        # Enviar respuesta al driver
                        try:
                            conn.sendall((json.dumps(response) + "\n").encode("utf-8"))
                        except:
                            pass
                    
                    # ✅ NUEVO: Cancelación de servicio por Driver
                    elif mtype == "cancel_service":
                        driver_id = obj.get("driver_id")
                        cancel_cp_id = obj.get("cp_id")
                        
                        print(f"[CP SOCKET] 🛑 Cancelación de servicio: Driver {driver_id} en CP {cancel_cp_id}")
                        
                        # Obtener CP
                        from .data_manager import get_cp
                        target_cp = get_cp(cancel_cp_id)
                        
                        if target_cp and target_cp.get("current_driver") == driver_id:
                            # Enviar comando de stop al CP
                            stop_command = {
                                "type": "stop_supply",
                                "reason": "driver_disconnected"
                            }
                            self.send_command_to_cp(cancel_cp_id, stop_command)
                            
                            # Actualizar BD
                            update_cp(cancel_cp_id,
                                    state="ACTIVADO",
                                    current_driver=None,
                                    current_kw=0.0,
                                    total_kwh=0.0,
                                    current_euros=0.0)
                            
                            print(f"[CP SOCKET] ✅ Suministro cancelado en {cancel_cp_id}")
                            self._notify_web(f"Driver {driver_id} desconectado - Suministro cancelado en {cancel_cp_id}", 'warning')
                    
                    else:
                        # Mensaje desconocido - ignorar
                        pass

        except Exception as e:
            print("[CP SOCKET] ❌ Error en client handler:", e)
            traceback.print_exc()
        
        finally:
            # Cleanup al desconectar
            if cp_id:
                try:
                    # Limpiar todos los campos y marcar desconectado
                    update_cp(
                        cp_id, 
                        state="DESCONECTADO",
                        current_kw=0.0,
                        total_kwh=0.0,
                        current_euros=0.0,
                        current_driver=None
                    )
                    print(f"[CP SOCKET] 📴 CP {cp_id} marcado como DESCONECTADO en BD")
                    self._notify_web(f"CP {cp_id} desconectado", 'info')
                except Exception as e:
                    print(f"[CP SOCKET] ❌ Error al marcar {cp_id} como desconectado: {e}")
                
                with self._clients_lock:
                    if cp_id in self._clients:
                        del self._clients[cp_id]
                        print(f"[CP SOCKET] 🗑️  CP {cp_id} eliminado de lista de clientes")
            
            try:
                conn.close()
            except:
                pass
            
            print(f"[CP SOCKET] 🔌 Conexión finalizada {addr}")