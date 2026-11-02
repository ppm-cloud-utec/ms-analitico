"""Worker de alertas: convierte eventos de riesgo en notificaciones al PMO.

Su cola filtra solo presupuesto.excedido y recurso.sobreasignado. Publica un
mensaje legible en el topic ppm-alertas, al que se puede suscribir un correo
sin tocar codigo.
"""
import os
import json

import boto3

from src.idempotencia import ya_procesado

_sns = boto3.client("sns")
TOPIC_ALERTAS = os.environ["SNS_ALERTAS_ARN"]


def _mensaje(evento):
    d = evento.get("datos") or {}
    if evento["tipo"] == "presupuesto.excedido":
        return (
            f"PRESUPUESTO EN RIESGO - {evento['tenant_id']}",
            f"El proyecto {d.get('codigo_proyecto') or d.get('proyecto_id')} "
            f"ejecuto {d.get('ejecutado_pct')}% de su presupuesto "
            f"(S/ {d.get('total_gastado')} de S/ {d.get('monto_aprobado')}).\n"
            f"Supero el umbral de {d.get('umbral_pct')}% y quedo marcado en rojo.",
        )
    return (
        f"PERSONA SOBREASIGNADA - {evento['tenant_id']}",
        f"{d.get('nombre')} quedo al {d.get('ocupacion_pct')}% de ocupacion "
        f"({d.get('exceso_pct')}% por encima de su capacidad) "
        f"tras asignarse al proyecto {d.get('proyecto_id')}.",
    )


def manejar(event, context):
    enviadas, omitidas = 0, 0

    for registro in event["Records"]:
        sobre = json.loads(registro["body"])
        evento = json.loads(sobre["Message"]) if "Message" in sobre else sobre

        # Clave distinta a la de ingesta: el mismo evento se archiva Y se
        # notifica, son dos efectos independientes.
        if ya_procesado(f"alerta:{evento['evento_id']}"):
            omitidas += 1
            continue

        asunto, cuerpo = _mensaje(evento)
        _sns.publish(
            TopicArn=TOPIC_ALERTAS,
            Subject=asunto[:100],
            Message=cuerpo,
            MessageAttributes={
                "tenant_id": {"DataType": "String",
                              "StringValue": evento["tenant_id"]},
                "tipo": {"DataType": "String", "StringValue": evento["tipo"]},
            },
        )
        enviadas += 1

    return {"enviadas": enviadas, "omitidas": omitidas}
