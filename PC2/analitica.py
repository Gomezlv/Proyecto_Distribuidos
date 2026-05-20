from __future__ import annotations
import zmq
import json
import time
import logging
import threading
import sys
import os
import argparse



sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'PC1'))
from sensor_base import cargar_config, ts_ahora

sys.path.insert(0, os.path.dirname(__file__))
from reglas import ReglasTrafico, EstadoTrafico


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("Analitica")

CICLOS_AUSENCIA = 3


class ServicioAnalitica:


    def __init__(self, cfg: dict):
        self.cfg        = cfg
        red             = cfg["red"]
        self.secret_key = cfg["seguridad"]["secret_key"]

        self.reglas = ReglasTrafico.desde_config(cfg["reglas"])

        self.sensores_registry: set = {s["id"] for s in cfg["sensores"]}
        self.last_reading: dict     = {}
        self.sensor_intervalos: dict = {s["id"]: s["intervalo_seg"] for s in cfg["sensores"]}
        self.datos_interseccion: dict = {}
        self._activo = True

        self.ctx = zmq.Context()

        # SUB <- Broker (PC1)
        self.sub_broker = self.ctx.socket(zmq.SUB)
        self.sub_broker.setsockopt(zmq.RCVHWM, 2000)
        broker_ep = f"tcp://{red['pc1_ip']}:{red['broker_pub_port']}"
        self.sub_broker.connect(broker_ep)
        for topic in ("espira_inductiva", "camara", "gps"):
            self.sub_broker.setsockopt_string(zmq.SUBSCRIBE, topic)
        log.info(f"[ANALITICA] SUB Broker -> {broker_ep}")

        # PUSH -> BD Principal (PC3)
        self.push_bd_princ = self.ctx.socket(zmq.PUSH)
        self.push_bd_princ.setsockopt(zmq.SNDHWM, 5000)
        self.push_bd_princ.setsockopt(zmq.LINGER, 2000)
        bd_princ_ep = f"tcp://{red['pc3_ip']}:{red['bd_princ_port']}"
        self.push_bd_princ.connect(bd_princ_ep)
        log.info(f"[ANALITICA] PUSH BD Principal -> {bd_princ_ep}")

        # PUSH -> BD Replica (local PC2)
        self.push_bd_rep = self.ctx.socket(zmq.PUSH)
        self.push_bd_rep.setsockopt(zmq.SNDHWM, 5000)
        self.push_bd_rep.setsockopt(zmq.LINGER, 2000)
        bd_rep_ep = f"tcp://127.0.0.1:{red['bd_rep_port']}"
        self.push_bd_rep.connect(bd_rep_ep)
        log.info(f"[ANALITICA] PUSH BD Replica -> {bd_rep_ep}")

        # PUSH -> Control Semaforos (local PC2)
        self.push_semaf = self.ctx.socket(zmq.PUSH)
        self.push_semaf.setsockopt(zmq.SNDHWM, 500)
        semaf_ep = f"tcp://127.0.0.1:{red['semaf_port']}"
        self.push_semaf.connect(semaf_ep)
        log.info(f"[ANALITICA] PUSH Semaforos -> {semaf_ep}")

        # REP <- Monitoreo/Consulta (PC3)
        self.rep_comandos = self.ctx.socket(zmq.REP)
        self.rep_comandos.setsockopt(zmq.RCVHWM, 1000)
        self.rep_comandos.setsockopt(zmq.LINGER, 2000)
        rep_ep = f"tcp://0.0.0.0:{red['analitica_rep_port']}"
        self.rep_comandos.bind(rep_ep)
        log.info(f"[ANALITICA] REP Comandos -> {rep_ep}")

        time.sleep(0.5)

    def _imprimir_reglas(self) -> None:
        r = self.reglas
        log.info("=" * 60)
        log.info("[ANALITICA] REGLAS DE TRAFICO CARGADAS")
        log.info("=" * 60)
        log.info(f"  NORMAL      : Q < {r.Q_normal_max}"
                f"  AND  Vp > {r.Vp_normal_min}"
                f"  AND  D < {r.D_normal_max}"
                f"     -> verde {r.tiempo_verde_normal}s")
        log.info(f"  MODERADO    : (cualquier otro caso)"
                f"                "
                f"  -> verde {r.tiempo_verde_moderado}s")
        log.info(f"  CONGESTION  : Q >= {r.Q_congestion}"
                f"  OR   Vp < {r.Vp_congestion}"
                f"  OR   D >= {r.D_normal_max * 2}"
                f"  -> verde {r.tiempo_verde_congestion}s")
        log.info(f"  SEVERO      : Q >= {r.Q_severo}"
                f"  OR   Vp < {r.Vp_severo}"
                f"           "
                f"      -> verde {r.tiempo_verde_severo}s")
        log.info(f"  PRIORIZACION: solicitud manual del operador"
                f"          -> verde duracion por parametro")
        log.info("=" * 60)

    def _sensor_autorizado(self, sensor_id: str) -> bool:
        if sensor_id not in self.sensores_registry:
            log.warning(f"[ANALITICA] Sensor NO autorizado: {sensor_id!r} — ignorado")
            return False
        return True

    def _actualizar_datos_interseccion(self, evento: dict) -> None:
        inters = evento.get("interseccion", "")
        if inters not in self.datos_interseccion:
            self.datos_interseccion[inters] = {"Q": 0, "Vp": 50.0, "D": 0}

        tipo = evento.get("tipo_sensor", "")
        if tipo == "camara":
            self.datos_interseccion[inters]["Q"]  = evento.get("volumen", 0)
            self.datos_interseccion[inters]["Vp"] = evento.get("velocidad_promedio", 50.0)
        elif tipo == "espira_inductiva":
            self.datos_interseccion[inters]["D"]  = evento.get("vehiculos_contados", 0)
        elif tipo == "gps":
            vp_gps    = evento.get("velocidad_promedio", 50.0)
            vp_actual = self.datos_interseccion[inters]["Vp"]
            self.datos_interseccion[inters]["Vp"] = round((vp_actual + vp_gps) / 2, 1)

    def _evaluar_congestion(self, interseccion: str) -> tuple:
        datos = self.datos_interseccion.get(interseccion, {"Q": 0, "Vp": 50.0, "D": 0})
        Q, Vp, D = datos["Q"], datos["Vp"], datos["D"]
        estado   = self.reglas.evaluar(Q, Vp, D)
        return estado, Q, Vp, D

    def _enviar_comando_semaforo(self, interseccion: str, estado: str,
                                  duracion: int, motivo: str) -> None:
        cmd = {
            "tipo_msg":     "semaforo",
            "interseccion": interseccion,
            "estado":       estado,
            "duracion_seg": duracion,
            "motivo":       motivo,
            "token":        self.secret_key,
            "timestamp":    ts_ahora()
        }
        self.push_semaf.send(json.dumps(cmd).encode())
        self._persistir(cmd)

    def _aplicar_orden_manual(self, cmd: dict, accion: str) -> dict:
        interseccion = cmd.get("interseccion", "")
        if not interseccion:
            return {"ok": False, "error": "Falta 'interseccion'."}

        motivo = cmd.get("motivo", "indicacion_directa")
        duracion = int(cmd.get("duracion_seg", self.reglas.tiempo_verde_severo))

        if accion == "forzar_semaforo":
            estado = cmd.get("estado", "VERDE").upper()
            if estado not in ("VERDE", "ROJO"):
                return {"ok": False, "error": "Estado debe ser VERDE o ROJO."}
            etiqueta_alerta = "FORZADO_MANUAL"
            accion_tomada = f"Semaforo forzado a {estado} por {motivo}"
        elif accion == "extender_verde":
            estado = "VERDE"
            etiqueta_alerta = "EXTENSION_VERDE"
            accion_tomada = f"Verde extendido {duracion}s — {motivo}"
        else:
            estado = "VERDE"
            etiqueta_alerta = "PRIORIZACION"
            accion_tomada = f"Priorizacion manual por {motivo}"

        self._log_diagnostico(
            interseccion,
            EstadoTrafico.PRIORIZACION,
            self.datos_interseccion.get(interseccion, {}).get("Q", 0),
            self.datos_interseccion.get(interseccion, {}).get("Vp", 50.0),
            self.datos_interseccion.get(interseccion, {}).get("D", 0),
            duracion,
        )
        log.info(
            f"[ANALITICA] ORDEN MANUAL {accion} | {interseccion} | "
            f"{estado} {duracion}s | {motivo}"
        )

        self._enviar_comando_semaforo(interseccion, estado, duracion, motivo)
        alerta = {
            "tipo_msg": "alerta",
            "interseccion": interseccion,
            "nivel": etiqueta_alerta,
            "accion_tomada": accion_tomada,
            "Q": self.datos_interseccion.get(interseccion, {}).get("Q", 0),
            "Vp": self.datos_interseccion.get(interseccion, {}).get("Vp", 50.0),
            "D": self.datos_interseccion.get(interseccion, {}).get("D", 0),
            "timestamp": ts_ahora(),
        }
        self._persistir(alerta)
        return {
            "ok": True,
            "mensaje": "Orden aplicada.",
            "accion": accion,
            "interseccion": interseccion,
        }

    def _handle_comando(self, cmd: dict) -> dict:
        accion = cmd.get("accion", "")
        if accion not in ("priorizar", "extender_verde", "forzar_semaforo"):
            return {"ok": False, "error": f"Accion no soportada: {accion!r}"}
        if cmd.get("token") != self.secret_key:
            return {"ok": False, "error": "Token invalido."}
        return self._aplicar_orden_manual(cmd, accion)

    def _hilo_comandos(self) -> None:
        while self._activo:
            try:
                raw = self.rep_comandos.recv(flags=zmq.NOBLOCK)
                try:
                    cmd = json.loads(raw.decode())
                    respuesta = self._handle_comando(cmd)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    respuesta = {"ok": False, "error": f"JSON invalido: {e}"}
                self.rep_comandos.send_json(respuesta)
            except zmq.Again:
                time.sleep(0.05)

    def _persistir(self, mensaje: dict) -> None:
        """Persiste en BD principal (PC3) y BD réplica (PC2) al mismo tiempo."""
        payload = json.dumps(mensaje).encode()
        ok_princ = ok_rep = False
        try:
            self.push_bd_princ.send(payload, flags=zmq.NOBLOCK)
            ok_princ = True
        except zmq.Again:
            log.warning("[ANALITICA] BD Principal HWM lleno — reintentar en ciclo siguiente")
        try:
            self.push_bd_rep.send(payload, flags=zmq.NOBLOCK)
            ok_rep = True
        except zmq.Again:
            log.warning("[ANALITICA] BD Replica HWM lleno")
        if not ok_princ or not ok_rep:
            log.warning(
                f"[ANALITICA] Persistencia dual incompleta "
                f"(principal={ok_princ}, replica={ok_rep}) "
                f"| {mensaje.get('tipo_msg')} {mensaje.get('interseccion', mensaje.get('sensor_id', ''))}"
            )

    def _log_diagnostico(self, interseccion: str, estado: EstadoTrafico,
                          Q: float, Vp: float, D: float, duracion: int) -> None:
        etiquetas = {
            EstadoTrafico.NORMAL:       "[NORMAL]      ",
            EstadoTrafico.MODERADO:     "[MODERADO]    ",
            EstadoTrafico.CONGESTION:   "[CONGESTION]  ",
            EstadoTrafico.SEVERO:       "[SEVERO]      ",
            EstadoTrafico.PRIORIZACION: "[PRIORIZACION]",
        }
        etiqueta = etiquetas.get(estado, "[?]")
        log.info(
            f"{etiqueta} {interseccion} | "
            f"Q={Q}, Vp={Vp} km/h, D={D} -> {estado.value} | "
            f"Verde: {duracion}s"
        )

    def procesar_evento(self, evento: dict) -> None:

        sensor_id = evento.get("sensor_id", "")
        if not self._sensor_autorizado(sensor_id):
            return

        self.last_reading[sensor_id] = {"ts": time.time()}
        self._actualizar_datos_interseccion(evento)
        interseccion = evento.get("interseccion", "")

        estado, Q, Vp, D = self._evaluar_congestion(interseccion)
        duracion = self.reglas.calcular_duracion_verde(estado)

        self._log_diagnostico(interseccion, estado, Q, Vp, D, duracion)

        if estado in (EstadoTrafico.CONGESTION, EstadoTrafico.SEVERO):
            alerta = {
                "tipo_msg":     "alerta",
                "interseccion": interseccion,
                "nivel":        estado.value,
                "accion_tomada": f"Verde extendido a {duracion}s",
                "Q": Q, "Vp": Vp, "D": D,
                "timestamp":    ts_ahora()
            }
            self._persistir(alerta)

        self._enviar_comando_semaforo(interseccion, "VERDE", duracion, estado.value)

        evento_persist = dict(evento)
        evento_persist["tipo_msg"] = "evento"
        self._persistir(evento_persist)


    def _hilo_detectar_ausentes(self) -> None:
        while self._activo:
            time.sleep(10)
            ahora = time.time()
            for sid, info in list(self.last_reading.items()):
                intervalo = self.sensor_intervalos.get(sid, 30)
                umbral    = intervalo * CICLOS_AUSENCIA
                if ahora - info["ts"] > umbral:
                    log.warning(
                        f"[ANALITICA] SENSOR OFFLINE: {sid} "
                        f"(sin datos por > {umbral}s)"
                    )


    def ejecutar(self) -> None:
        hilo_ausentes = threading.Thread(
            target=self._hilo_detectar_ausentes,
            name="hilo-ausentes",
            daemon=True
        )
        hilo_ausentes.start()

        hilo_cmd = threading.Thread(
            target=self._hilo_comandos,
            name="hilo-comandos",
            daemon=True
        )
        hilo_cmd.start()

        self._imprimir_reglas()

        log.info("[ANALITICA] Servicio iniciado. Esperando eventos del Broker...")
        try:
            while self._activo:
                try:
                    partes = self.sub_broker.recv_multipart(flags=zmq.NOBLOCK)
                    if len(partes) == 2:
                        _, payload_b = partes
                        try:
                            evento = json.loads(payload_b.decode())
                            self.procesar_evento(evento)
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            log.warning(f"[ANALITICA] JSON invalido del Broker: {e}")
                except zmq.Again:
                    time.sleep(0.02)
        except KeyboardInterrupt:
            log.info("[ANALITICA] Detenido por usuario.")
        finally:
            self.cerrar()

    def cerrar(self) -> None:
        self._activo = False
        for sock in (self.sub_broker, self.push_bd_princ,
                     self.push_bd_rep, self.push_semaf,
                     self.rep_comandos):
            sock.close()
        self.ctx.term()
        log.info("[ANALITICA] Recursos ZMQ liberados.")


def main():
    parser = argparse.ArgumentParser(description="Servicio de Analitica — PC2")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    cfg      = cargar_config(args.config)
    servicio = ServicioAnalitica(cfg)
    servicio.ejecutar()


if __name__ == "__main__":
    main()
