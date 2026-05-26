from __future__ import annotations
import zmq
import json
import time
import logging
import threading
import argparse

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import cargar_config
from common.coordinacion import CoordinadorSemaforos
from common.pc3_state import pc3_activo
from common.protocol import OP_ANALITICA_PRIORIDAD
from common.time_utils import ts_ahora
from reglas import ReglasTrafico, EstadoTrafico

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("Analitica")

CICLOS_AUSENCIA = 3


class ServicioAnalitica:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        red = cfg["red"]
        self.secret_key = cfg["seguridad"]["secret_key"]
        self.reglas = ReglasTrafico.desde_config(cfg["reglas"])
        self.coordinador = CoordinadorSemaforos(cfg)
        self.semaforos_cfg = set(cfg.get("semaforos", []))
        self.sensores_registry = {s["id"] for s in cfg["sensores"]}
        self.last_reading = {}
        self.sensor_intervalos = {s["id"]: s["intervalo_seg"] for s in cfg["sensores"]}
        self.datos_interseccion = {}
        self._activo = True
        self._descartes_princ = 0

        self.ctx = zmq.Context()
        poller = zmq.Poller()

        self.sub_broker = self.ctx.socket(zmq.SUB)
        self.sub_broker.setsockopt(zmq.RCVHWM, 2000)
        broker_ep = f"tcp://{red['pc1_ip']}:{red['broker_pub_port']}"
        self.sub_broker.connect(broker_ep)
        for topic in ("espira_inductiva", "camara", "gps"):
            self.sub_broker.setsockopt_string(zmq.SUBSCRIBE, topic)
        poller.register(self.sub_broker, zmq.POLLIN)
        log.info("[ANALITICA] SUB Broker -> %s", broker_ep)

        self.push_bd_princ = self.ctx.socket(zmq.PUSH)
        self.push_bd_princ.setsockopt(zmq.SNDHWM, 5000)
        self.push_bd_princ.setsockopt(zmq.LINGER, 2000)
        self.push_bd_princ.connect(f"tcp://{red['pc3_ip']}:{red['bd_princ_port']}")

        self.push_bd_rep = self.ctx.socket(zmq.PUSH)
        self.push_bd_rep.setsockopt(zmq.SNDHWM, 5000)
        self.push_bd_rep.setsockopt(zmq.LINGER, 2000)
        self.push_bd_rep.connect(f"tcp://{red['pc2_ip']}:{red['bd_rep_port']}")

        self.push_semaf = self.ctx.socket(zmq.PUSH)
        self.push_semaf.setsockopt(zmq.SNDHWM, 500)
        self.push_semaf.connect(f"tcp://{red['pc2_ip']}:{red['semaf_port']}")

        self.rep_cmd = self.ctx.socket(zmq.REP)
        cmd_port = red.get("analytics_cmd_port", 5565)
        self.rep_cmd.bind(f"tcp://0.0.0.0:{cmd_port}")
        poller.register(self.rep_cmd, zmq.POLLIN)
        log.info("[ANALITICA] REP comandos en tcp://0.0.0.0:%s", cmd_port)

        self._poller = poller
        time.sleep(0.5)

    def _imprimir_reglas(self) -> None:
        r = self.reglas
        log.info("=" * 60)
        log.info("[ANALITICA] REGLAS DE TRAFICO CARGADAS")
        log.info("  NORMAL: Q<%s AND Vp>%s AND D<%s", r.Q_normal_max, r.Vp_normal_min, r.D_normal_max)
        log.info("  Mapeo: Q=camara, Vp=camara/gps, D=gps/veh-min(espira)")
        log.info("  PRIORIZACION: solo comando manual (monitor)")
        log.info("=" * 60)

    def _sensor_autorizado(self, sensor_id: str) -> bool:
        if sensor_id not in self.sensores_registry:
            log.warning("[ANALITICA] Sensor NO autorizado: %s", sensor_id)
            return False
        return True

    def _densidad_desde_gps(self, evento: dict) -> float:
        nivel = evento.get("nivel_congestion", "NORMAL")
        return {"ALTA": 35.0, "NORMAL": 15.0, "BAJA": 5.0}.get(nivel, 15.0)

    def _actualizar_datos_interseccion(self, evento: dict) -> None:
        inters = evento.get("interseccion", "")
        if inters not in self.datos_interseccion:
            self.datos_interseccion[inters] = {"Q": 0, "Vp": 50.0, "D": 0}

        tipo = evento.get("tipo_sensor", "")
        if tipo == "camara":
            self.datos_interseccion[inters]["Q"] = evento.get("volumen", 0)
            self.datos_interseccion[inters]["Vp"] = evento.get("velocidad_promedio", 50.0)
        elif tipo == "espira_inductiva":
            veh = evento.get("vehiculos_contados", 0)
            intervalo = max(evento.get("intervalo_segundos", 30), 1)
            v_flujo = round(veh / (intervalo / 60.0), 1)
            self.datos_interseccion[inters]["D"] = v_flujo
        elif tipo == "gps":
            vp_gps = evento.get("velocidad_promedio", 50.0)
            vp_actual = self.datos_interseccion[inters]["Vp"]
            self.datos_interseccion[inters]["Vp"] = round((vp_actual + vp_gps) / 2, 1)
            self.datos_interseccion[inters]["D"] = self._densidad_desde_gps(evento)

    def _evaluar_congestion(self, interseccion: str) -> tuple:
        datos = self.datos_interseccion.get(interseccion, {"Q": 0, "Vp": 50.0, "D": 0})
        return self.reglas.evaluar(datos["Q"], datos["Vp"], datos["D"]), datos["Q"], datos["Vp"], datos["D"]

    def _enviar_comando_semaforo(self, interseccion: str, estado: str, duracion: int, motivo: str) -> float:
        cmd = {
            "tipo_msg": "semaforo",
            "interseccion": interseccion,
            "estado": estado,
            "duracion_seg": duracion,
            "motivo": motivo,
            "token": self.secret_key,
            "timestamp": ts_ahora(),
            "alias": self.coordinador.alias(interseccion),
        }
        self.push_semaf.send(json.dumps(cmd).encode())
        self._persistir(cmd)
        return time.time()

    def _modo_rojo(self, motivo: str) -> str:
        if motivo in (EstadoTrafico.CONGESTION.value, EstadoTrafico.SEVERO.value):
            return "congestion"
        if motivo == EstadoTrafico.PRIORIZACION.value:
            return "priorizacion"
        return "normal"

    def _aplicar_rojos_conflictivos(self, interseccion: str, motivo: str) -> None:
        modo = self._modo_rojo(motivo)
        duracion_rojo = self.coordinador.duracion_rojo(modo)
        for conflicto in self.coordinador.conflictos_de(interseccion):
            if conflicto not in self.semaforos_cfg:
                continue
            self._enviar_comando_semaforo(
                conflicto, "ROJO", duracion_rojo, f"conflicto_{motivo}",
            )

    def _enviar_verde_coordinado(self, interseccion: str, duracion: int, motivo: str) -> float:
        self._aplicar_rojos_conflictivos(interseccion, motivo)
        return self._enviar_comando_semaforo(interseccion, "VERDE", duracion, motivo)

    def _persistir(self, mensaje: dict) -> None:
        payload = json.dumps(mensaje, ensure_ascii=False).encode()
        try:
            self.push_bd_rep.send(payload, flags=zmq.NOBLOCK)
        except zmq.Again:
            log.warning("[ANALITICA] BD Replica HWM lleno")

        if pc3_activo():
            try:
                self.push_bd_princ.send(payload)
            except zmq.Again:
                self._descartes_princ += 1
                log.warning(
                    "[ANALITICA] BD Principal HWM lleno (total descartes=%s)",
                    self._descartes_princ,
                )
        else:
            log.debug("[ANALITICA] PC3 caído — solo réplica")

    def procesar_comando_manual(self, req: dict) -> dict:
        t0 = time.time()
        if req.get("token") != self.secret_key:
            return {"ok": False, "error": "token inválido"}
        inter = req.get("interseccion", "")
        duracion = int(req.get("duracion_seg", 60))
        if not inter:
            return {"ok": False, "error": "interseccion requerida"}

        log.info("[ANALITICA] PRIORIZACION manual %s %ss", inter, duracion)
        t_sem = self._enviar_verde_coordinado(inter, duracion, EstadoTrafico.PRIORIZACION.value)
        alerta = {
            "tipo_msg": "alerta",
            "interseccion": inter,
            "nivel": "PRIORIZACION",
            "accion_tomada": f"Verde prioritario {duracion}s (ambulancia)",
            "timestamp": ts_ahora(),
        }
        self._persistir(alerta)
        return {
            "ok": True,
            "interseccion": inter,
            "t_recibido": t0,
            "t_semaforo_enviado": t_sem,
            "latencia_seg": round(t_sem - t0, 4),
        }

    def procesar_evento(self, evento: dict) -> None:
        sensor_id = evento.get("sensor_id", "")
        if not self._sensor_autorizado(sensor_id):
            return
        self.last_reading[sensor_id] = {"ts": time.time()}
        self._actualizar_datos_interseccion(evento)
        interseccion = evento.get("interseccion", "")
        estado, Q, Vp, D = self._evaluar_congestion(interseccion)
        duracion = self.reglas.calcular_duracion_verde(estado)
        log.info(
            "[%s] %s | Q=%s Vp=%s D=%s -> verde %ss",
            estado.value, interseccion, Q, Vp, D, duracion,
        )
        if estado in (EstadoTrafico.CONGESTION, EstadoTrafico.SEVERO):
            self._persistir({
                "tipo_msg": "alerta",
                "interseccion": interseccion,
                "nivel": estado.value,
                "accion_tomada": f"Verde extendido a {duracion}s",
                "Q": Q, "Vp": Vp, "D": D,
                "timestamp": ts_ahora(),
            })
        self._enviar_verde_coordinado(interseccion, duracion, estado.value)
        ev = dict(evento)
        ev["tipo_msg"] = "evento"
        self._persistir(ev)

    def _hilo_detectar_ausentes(self) -> None:
        while self._activo:
            time.sleep(10)
            ahora = time.time()
            for sid, info in list(self.last_reading.items()):
                umbral = self.sensor_intervalos.get(sid, 30) * CICLOS_AUSENCIA
                if ahora - info["ts"] > umbral:
                    log.warning("[ANALITICA] SENSOR OFFLINE: %s", sid)

    def ejecutar(self) -> None:
        threading.Thread(target=self._hilo_detectar_ausentes, daemon=True).start()
        self._imprimir_reglas()
        log.info("[ANALITICA] Servicio iniciado.")
        try:
            while self._activo:
                eventos = dict(self._poller.poll(timeout=500))
                if self.rep_cmd in eventos:
                    try:
                        req = self.rep_cmd.recv_json(zmq.NOBLOCK)
                        if req.get("operacion") == OP_ANALITICA_PRIORIDAD:
                            resp = self.procesar_comando_manual(req)
                        else:
                            resp = {"ok": False, "error": "operacion no soportada"}
                        self.rep_cmd.send_json(resp)
                    except zmq.Again:
                        pass
                if self.sub_broker in eventos:
                    partes = self.sub_broker.recv_multipart(flags=zmq.NOBLOCK)
                    if len(partes) == 2:
                        try:
                            self.procesar_evento(json.loads(partes[1].decode()))
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            log.warning("[ANALITICA] JSON inválido: %s", e)
        except KeyboardInterrupt:
            log.info("[ANALITICA] Detenido.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        for sock in (self.sub_broker, self.push_bd_princ, self.push_bd_rep, self.push_semaf, self.rep_cmd):
            sock.close()
        self.ctx.term()


def main():
    parser = argparse.ArgumentParser(description="Servicio de Analitica — PC2")
    parser.add_argument("--config", default="../PC1/config.json")
    args = parser.parse_args()
    cfg = cargar_config(args.config)
    ServicioAnalitica(cfg).ejecutar()


if __name__ == "__main__":
    main()
