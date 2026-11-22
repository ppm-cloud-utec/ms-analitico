"""Worker de ingesta: TODOS los eventos del sistema entran por aqui."""
import json

from src import lago
from src.idempotencia import liberar, ya_procesado

CONTADORES = {
    "proyecto.creado":       lambda d: {"proyectos_creados": 1,
                                        "presupuesto_comprometido": d.get("presupuesto_estimado", 0)},
    "proyecto.priorizado":   lambda d: {"repriorizaciones": 1},
    "tarea.creada":          lambda d: {"tareas_creadas": 1},
    "tarea.completada":      lambda d: {"tareas_completadas": 1},
    "recurso.asignado":      lambda d: {"asignaciones": 1,
                                        "horas_comprometidas": d.get("horas_semana", 0)},
    "recurso.sobreasignado": lambda d: {"sobreasignaciones": 1},
    "gasto.registrado":      lambda d: {"gastos": 1, "monto_gastado": d.get("monto", 0)},
    "presupuesto.excedido":  lambda d: {"presupuestos_excedidos": 1},
}


def manejar(event, context):
    archivados, sumados, omitidos = 0, 0, 0

    for registro in event["Records"]:
        sobre = json.loads(registro["body"])
        evento = json.loads(sobre["Message"]) if "Message" in sobre else sobre

        if ya_procesado(evento["evento_id"]):
            omitidos += 1
            continue

        try:
            lago.guardar_evento(evento)
            archivados += 1

            constructor = CONTADORES.get(evento["tipo"])
            if constructor:
                contadores = constructor(evento.get("datos") or {})
                contadores["eventos"] = 1
                mes = (evento.get("ocurrido_en") or "")[:7] or "sin-fecha"
                lago.sumar(evento["tenant_id"], mes, contadores)
                lago.sumar(evento["tenant_id"], "TOTAL", contadores)
                sumados += 1
        except Exception:
            liberar(evento["evento_id"])
            raise

    return {"archivados": archivados, "sumados": sumados, "omitidos": omitidos}
