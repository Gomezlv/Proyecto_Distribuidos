from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import threading
import time

import zmq

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'PC1'))
sys.path.insert(0, os.path.dirname(__file__))
from sensor_base import cargar_config
from zmq_util import crear_req, req_json


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("MonitoreoConsulta")


class ClienteBDFailover:
    """REQ hacia BD principal (PC3); failover transparente a BD réplica (PC2)."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        red = cfg["red"]
        hc = cfg["health_check"]
        self._timeout_ms = hc["timeout_ms"]
        self._max_intentos = hc["max_intentos"]
        self._intervalo = hc["intervalo_seg"]

        self._ctx = zmq.Context()
        self._ep_princ = f"tcp://{red['pc3_ip']}:{red['bd_princ_query_port']}"
        self._ep_rep = f"tcp://{red['pc2_ip']}:{red['bd_replica_query_port']}"
        self._sock_princ: zmq.Socket | None = None
        self._sock_rep: zmq.Socket | None = None
        self._lock = threading.Lock()
        self._backend = "principal"
        self._fallos_princ = 0
        self._activo = True

    def _log(self, msg: str) -> None:
        log.info(f"[MONITOREO-BD] {msg}")

    def _req(self, ep: str, sock_attr: str, mensaje: dict) -> tuple[dict | None, zmq.Socket | None]:
        with self._lock:
            sock = getattr(self, sock_attr)
            sock, resp = req_json(self._ctx, sock, ep, mensaje, self._timeout_ms)
            setattr(self, sock_attr, sock)
            return resp, sock

    def ping_principal(self) -> bool:
        resp, _ = self._req(self._ep_princ, "_sock_princ", {"accion": "ping"})
        return resp is not None and resp.get("ok")

    def consulta(self, req: dict) -> dict:
        """Consulta con failover: principal primero, réplica si falla."""
        resp, _ = self._req(self._ep_princ, "_sock_princ", req)
        if resp is not None and resp.get("ok"):
            self._fallos_princ = 0
            if self._backend != "principal":
                self._log("BD principal recuperada (failback)")
            self._backend = "principal"
            return resp

        self._fallos_princ += 1
        resp_rep, _ = self._req(self._ep_rep, "_sock_rep", req)
        if resp_rep is not None and resp_rep.get("ok"):
            if self._backend != "replica":
                self._log("Failover transparente -> BD réplica")
            self._backend = "replica"
            return resp_rep

        return {
            "ok": False,
            "error": "BD principal y réplica no disponibles",
            "backend_intentado": self._backend,
        }

    def _hilo_health(self) -> None:
        while self._activo:
            time.sleep(self._intervalo)
            if self.ping_principal():
                continue
            self._log(
                f"Health check: principal sin respuesta "
                f"({self._fallos_princ}/{self._max_intentos})"
            )

    def iniciar_health(self) -> None:
        threading.Thread(
            target=self._hilo_health,
            daemon=True,
            name="monitoreo-health",
        ).start()

    def cerrar(self) -> None:
        self._activo = False
        for attr in ("_sock_princ", "_sock_rep"):
            s = getattr(self, attr)
            if s is not None:
                s.close()
        self._ctx.term()


class ServicioMonitoreoConsulta:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._activo = True
        self.secret_key = cfg["seguridad"]["secret_key"]
        red = cfg["red"]
        self._timeout_ms = cfg["health_check"]["timeout_ms"]

        self.cliente_bd = ClienteBDFailover(cfg)
        self.cliente_bd.iniciar_health()

        self.ctx = zmq.Context()
        self.rep = self.ctx.socket(zmq.REP)
        self.rep.setsockopt(zmq.RCVHWM, 1000)
        self.rep.setsockopt(zmq.LINGER, 2000)
        endpoint = f"tcp://0.0.0.0:{red['monitor_rep_port']}"
        self.rep.bind(endpoint)
        log.info(f"[MONITOREO] REP operador -> {endpoint}")

        self._ep_analitica = f"tcp://{red['pc2_ip']}:{red['analitica_rep_port']}"
        self._sock_analitica: zmq.Socket | None = None

    def _log(self, msg: str) -> None:
        log.info(f"[MONITOREO] {msg}")

    def _analitica(self, mensaje: dict) -> dict:
        self._sock_analitica, resp = req_json(
            self.ctx,
            self._sock_analitica,
            self._ep_analitica,
            mensaje,
            self._timeout_ms,
        )
        if resp is None:
            return {"ok": False, "error": "Analitica no disponible (timeout)"}
        return resp

    def _orden_analitica(self, accion: str, payload: dict) -> dict:
        if payload.get("token") != self.secret_key:
            return {"ok": False, "error": "Token invalido."}
        if not payload.get("interseccion"):
            return {"ok": False, "error": "Falta 'interseccion'."}

        mensaje = {
            "accion": accion,
            "interseccion": payload.get("interseccion", ""),
            "duracion_seg": payload.get("duracion_seg", 60),
            "motivo": payload.get("motivo", "indicacion_externa"),
            "estado": payload.get("estado", "VERDE"),
            "token": self.secret_key,
        }
        self._log(f"Orden a analitica: {accion} -> {mensaje['interseccion']}")
        respuesta = self._analitica(mensaje)
        self._log(f"Respuesta analitica: {respuesta}")
        if not respuesta.get("ok"):
            return {
                "ok": False,
                "tipo": accion,
                "error": respuesta.get("error", "Analitica rechazo la orden"),
                "respuesta_analitica": respuesta,
            }
        return {"ok": True, "tipo": accion, "respuesta_analitica": respuesta}

    def _handle(self, req: dict) -> dict:
        accion = req.get("accion", "")

        if accion in ("estado_sistema", "consulta_historica", "consulta_interseccion"):
            resp = self.cliente_bd.consulta(req)
            if resp.get("ok"):
                resp["backend_bd"] = self.cliente_bd._backend
            return resp

        if accion in ("priorizar", "extender_verde", "forzar_semaforo"):
            return self._orden_analitica(accion, req)

        return {"ok": False, "error": f"Accion desconocida: {accion!r}"}

    def ejecutar(self) -> None:
        self._log("Servicio iniciado (REQ->BD con failover, REQ->Analitica).")
        try:
            while self._activo:
                try:
                    raw = self.rep.recv(flags=zmq.NOBLOCK)
                    try:
                        req = json.loads(raw.decode())
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        self.rep.send_json({"ok": False, "error": f"JSON invalido: {e}"})
                        continue
                    self._log(f"Operacion recibida: {req.get('accion')}")
                    resp = self._handle(req)
                    self.rep.send_json(resp)
                except zmq.Again:
                    time.sleep(0.05)
        except KeyboardInterrupt:
            self._log("Detenido por usuario.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        self.rep.close()
        if self._sock_analitica is not None:
            self._sock_analitica.close()
        self.cliente_bd.cerrar()
        self.ctx.term()
        self._log("Recursos liberados.")


def _prompt(texto: str, default: str | None = None) -> str:
    sufijo = f" [{default}]" if default is not None else ""
    valor = input(f"{texto}{sufijo}: ").strip()
    if not valor and default is not None:
        return default
    return valor


def _imprimir_json(data: dict) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _cliente_menu(cfg: dict) -> None:
    red = cfg["red"]
    timeout_ms = cfg["health_check"]["timeout_ms"]
    endpoint = f"tcp://127.0.0.1:{red['monitor_rep_port']}"
    ctx = zmq.Context()
    sock = crear_req(ctx, endpoint, timeout_ms)
    print(f"[MENU] Conectado a {endpoint}")

    try:
        while True:
            print("\n=== Monitoreo y Consulta ===")
            print("1. Estado del sistema")
            print("2. Consulta historica")
            print("3. Consulta por interseccion")
            print("4. Priorizar (ambulancia / emergencia)")
            print("5. Extender verde")
            print("6. Forzar cambio de semaforo")
            print("7. Salir")
            opcion = input("Seleccione una opcion: ").strip()

            if opcion == "1":
                req = {"accion": "estado_sistema"}
            elif opcion == "2":
                desde = _prompt("Desde (ISO8601)", "2026-01-01T00:00:00Z")
                hasta = _prompt("Hasta (ISO8601)", "2026-12-31T23:59:59Z")
                req = {"accion": "consulta_historica", "desde": desde, "hasta": hasta}
            elif opcion == "3":
                inter = _prompt("Interseccion", "INT-B4")
                req = {"accion": "consulta_interseccion", "interseccion": inter}
            elif opcion == "4":
                inter = _prompt("Interseccion", "INT-C3")
                dur = int(_prompt("Duracion en segundos", "45"))
                motivo = _prompt("Motivo", "ambulancia")
                token = _prompt("Token", cfg["seguridad"]["secret_key"])
                req = {
                    "accion": "priorizar",
                    "interseccion": inter,
                    "duracion_seg": dur,
                    "motivo": motivo,
                    "token": token,
                }
            elif opcion == "5":
                inter = _prompt("Interseccion", "INT-B4")
                dur = int(_prompt("Segundos extra de verde", "20"))
                token = _prompt("Token", cfg["seguridad"]["secret_key"])
                req = {
                    "accion": "extender_verde",
                    "interseccion": inter,
                    "duracion_seg": dur,
                    "motivo": "extension_manual",
                    "token": token,
                }
            elif opcion == "6":
                inter = _prompt("Interseccion", "INT-A2")
                estado = _prompt("Estado (VERDE/ROJO)", "VERDE").upper()
                dur = int(_prompt("Duracion en segundos", "30"))
                token = _prompt("Token", cfg["seguridad"]["secret_key"])
                req = {
                    "accion": "forzar_semaforo",
                    "interseccion": inter,
                    "estado": estado,
                    "duracion_seg": dur,
                    "motivo": "forzado_manual",
                    "token": token,
                }
            elif opcion == "7":
                print("[MENU] Saliendo.")
                break
            else:
                print("[MENU] Opcion invalida.")
                continue

            print(f"[MENU] Enviando: {req}")
            sock, resp = req_json(ctx, sock, endpoint, req, timeout_ms)
            if resp is None:
                print("[MENU] Sin respuesta (timeout).")
            else:
                print("[MENU] Respuesta:")
                _imprimir_json(resp)
    finally:
        sock.close()
        ctx.term()


def main():
    parser = argparse.ArgumentParser(description="Monitoreo y Consulta — PC3")
    parser.add_argument("--config", default="../PC1/config.json")
    parser.add_argument("--menu", action="store_true", help="Menu interactivo (cliente REQ)")
    args = parser.parse_args()

    cfg = cargar_config(args.config)
    if args.menu:
        _cliente_menu(cfg)
        return
    ServicioMonitoreoConsulta(cfg).ejecutar()


if __name__ == "__main__":
    main()
