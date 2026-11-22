"""Escritura en el data lake de S3 y lectura de los agregados."""
import os
import json
from datetime import datetime, timezone
from decimal import Decimal

import boto3

_s3 = boto3.client("s3")
_dynamo = boto3.resource("dynamodb")

BUCKET = os.environ["BUCKET_DATALAKE"]
TABLA_AGREGADOS = os.environ["TABLA_AGREGADOS"]


def agregados():
    return _dynamo.Table(TABLA_AGREGADOS)


def ruta(tenant_id, tipo, ocurrido_en, evento_id):
    """Clave del evento, particionada por tenant, fecha y tipo."""
    fecha = ocurrido_en[:10]
    return (f"eventos/tenant_id={tenant_id}/fecha={fecha}/"
            f"tipo={tipo}/{evento_id}.json")


def guardar_evento(evento):
    """Un objeto por evento, en JSON de una sola linea (lo que Athena espera)."""
    clave = ruta(evento["tenant_id"], evento["tipo"],
                 evento.get("ocurrido_en", datetime.now(timezone.utc).isoformat()),
                 evento["evento_id"])
    _s3.put_object(
        Bucket=BUCKET,
        Key=clave,
        Body=(json.dumps(aplanar(evento), ensure_ascii=False) + "\n").encode(),
        ContentType="application/json",
    )
    return clave


def aplanar(evento):
    """Aplana el evento a una sola capa para Athena."""
    plano = {
        "evento_id": evento["evento_id"],
        "tipo": evento["tipo"],
        "tenant_id": evento["tenant_id"],
        "origen": evento.get("origen"),
        "ocurrido_en": evento.get("ocurrido_en"),
        "version": evento.get("version", 1),
    }
    for clave, valor in (evento.get("datos") or {}).items():
        plano[clave] = valor if not isinstance(valor, (dict, list)) \
            else json.dumps(valor, ensure_ascii=False)
    return plano


def sumar(tenant_id, periodo, contadores):
    """Incrementa los contadores del periodo de forma atomica."""
    partes = ", ".join(f"#{k} :{k}" for k in contadores)
    return agregados().update_item(
        Key={"pk": f"TENANT#{tenant_id}", "sk": f"PERIODO#{periodo}"},
        UpdateExpression=f"ADD {partes}",
        ExpressionAttributeNames={f"#{k}": k for k in contadores},
        ExpressionAttributeValues={f":{k}": Decimal(str(v))
                                   for k, v in contadores.items()},
        ReturnValues="ALL_NEW",
    )["Attributes"]
