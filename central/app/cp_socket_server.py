import socket
import threading
import json
import traceback
from .data_manager import ensure_cp_exists, update_cp, get_all_cps, load_data

class CPSocketServer:
    """
    Servidor TCP sencillo que acepta conexiones de CP monitors.
    Protocolo: mensajes JSON por conexión (newline-optional). Cada recv puede contener JSON.
    Mensajes esperados:
      - register: {"type":"register","cp_id":"cp001","location":"...","price":0.35}
      - telemetry: {"type":"telemetry","cp_id":"cp001","is_supplying":true,"consumption_kw":2.5,"total_kwh":1.2,"current_price":0.35}
      - fault: {"type":"fault","cp_id":"cp001","msg":"..."}
      - disconnect: {"type":"disconnect","cp_id":"cp001"}
    """

    def __init__(self, host="0.0.0.0", port=5001, producer=None):
        self.host = host
        self.port = port
        self.producer = producer
        self._sock = None
        self._clients = {}  # cp_id -> (conn,addr)
        self._stop_flag = threading.Event()

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

    def _serve_forever(self):
        print(f"[CP SOCKET] Iniciando servidor en {self.host}:{self.port}")
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

                    # process obj
                    mtype = obj.get("type")
                    if mtype == "register":
                        cp_id = obj.get("cp_id")
                        location = obj.get("location")
                        price = obj.get("price_eur_kwh", obj.get("price", 0.0))
                        ensure_cp_exists(cp_id, location=location, price=price)
                        update_cp(cp_id, state="ACTIVADO", location=location, price=float(price))
                        print(f"[CP SOCKET] CP registrado: {cp_id} desde {addr}")
                        # store client
                        self._clients[cp_id] = (conn, addr)
                        # ack
                        ack = {"type":"register_ack","status":"ok"}
                        try:
                            conn.sendall((json.dumps(ack)+"\n").encode("utf-8"))
                        except:
                            pass
                    elif mtype == "telemetry":
                        cp_id = obj.get("cp_id")
                        if cp_id:
                            # update fields
                            fields = {}
                            if "is_supplying" in obj:
                                fields["state"] = "SUMINISTRANDO" if obj.get("is_supplying") else "ACTIVADO"
                            if "consumption_kw" in obj:
                                fields["current_kw"] = float(obj.get("consumption_kw") or 0.0)
                            if "total_kwh" in obj:
                                fields["total_kwh"] = float(obj.get("total_kwh") or 0.0)
                            if "current_price" in obj:
                                fields["current_euros"] = float(obj.get("current_price") or 0.0) * float(obj.get("total_kwh", 0.0))
                            update_cp(cp_id, **fields)
                            # publish monitor snapshot if producer exists
                            if self.producer:
                                snapshot = {"type":"telemetry","cp_id":cp_id,"payload":obj}
                                self.producer.publish_monitor(snapshot)
                    elif mtype == "fault":
                        cp_id = obj.get("cp_id")
                        update_cp(cp_id, state="AVERIADO")
                        if self.producer:
                            self.producer.publish_monitor({"type":"fault","cp_id":cp_id,"msg":obj.get("msg")})
                    elif mtype == "disconnect":
                        cp_id = obj.get("cp_id")
                        update_cp(cp_id, state="DESCONECTADO")
                        break
                    else:
                        # desconocido - ignorar
                        pass

        except Exception as e:
            print("[CP SOCKET] error client handler:", e)
            traceback.print_exc()
        finally:
            if cp_id:
                try:
                    update_cp(cp_id, state="DESCONECTADO")
                except:
                    pass
                if cp_id in self._clients:
                    del self._clients[cp_id]
            try:
                conn.close()
            except:
                pass
            print(f"[CP SOCKET] conexión finalizada {addr}")
