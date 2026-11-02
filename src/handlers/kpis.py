"""El dashboard ejecutivo: las 4 preguntas del producto en una pantalla.

    1. En que estamos gastando y cuanto queda?
    2. Quien esta sobrecargado?
    3. Que proyecto se va a caer?
    4. Que priorizamos el proximo trimestre?

Lee de la tabla de agregados (DynamoDB), que el worker de ingesta mantiene al
dia. No consulta Athena: una consulta de Athena tarda segundos y se cobra por
byte escaneado, y un panel no puede depender de eso.
"""
from datetime import datetime, timezone

from boto3.dynamodb.conditions import Key

from src import lago
from src.respuesta import ok, tenant_de


def _numeros(item):
    """Los contadores llegan como Decimal; se normalizan a int o float."""
    salida = {}
    for clave, valor in (item or {}).items():
        if clave in ("pk", "sk"):
            continue
        try:
            f = float(valor)
            salida[clave] = int(f) if f.is_integer() else round(f, 2)
        except (TypeError, ValueError):
            salida[clave] = valor
    return salida


def resumen(event, context):
    """GET /kpis — el acumulado del tenant."""
    tenant_id = tenant_de(event)
    item = lago.agregados().get_item(
        Key={"pk": f"TENANT#{tenant_id}", "sk": "PERIODO#TOTAL"}
    ).get("Item")

    datos = _numeros(item)
    creados = datos.get("proyectos_creados", 0)
    tareas = datos.get("tareas_creadas", 0)
    completadas = datos.get("tareas_completadas", 0)

    return ok({
        "tenant_id": tenant_id,
        "proyectos_creados": creados,
        "repriorizaciones": datos.get("repriorizaciones", 0),
        "tareas_creadas": tareas,
        "tareas_completadas": completadas,
        "avance_global_pct": round(100 * completadas / tareas, 2) if tareas else 0.0,
        "presupuesto_comprometido": datos.get("presupuesto_comprometido", 0),
        "monto_gastado": datos.get("monto_gastado", 0),
        "ejecucion_global_pct": round(
            100 * datos.get("monto_gastado", 0)
            / datos.get("presupuesto_comprometido", 1), 2
        ) if datos.get("presupuesto_comprometido") else 0.0,
        "gastos": datos.get("gastos", 0),
        "presupuestos_excedidos": datos.get("presupuestos_excedidos", 0),
        "asignaciones": datos.get("asignaciones", 0),
        "horas_comprometidas": datos.get("horas_comprometidas", 0),
        "sobreasignaciones": datos.get("sobreasignaciones", 0),
        "eventos_procesados": datos.get("eventos", 0),
    }, meta={"fuente": "agregados en DynamoDB, alimentados por eventos",
             "generado_en": datetime.now(timezone.utc).isoformat()})


def serie(event, context):
    """GET /kpis/serie — la evolucion mes a mes, para los graficos."""
    tenant_id = tenant_de(event)
    r = lago.agregados().query(
        KeyConditionExpression=Key("pk").eq(f"TENANT#{tenant_id}")
        & Key("sk").begins_with("PERIODO#20"),   # excluye PERIODO#TOTAL
    )
    meses = []
    for item in sorted(r.get("Items", []), key=lambda i: i["sk"]):
        datos = _numeros(item)
        datos["periodo"] = item["sk"].removeprefix("PERIODO#")
        meses.append(datos)
    return ok(meses, meta={"total": len(meses)})


def eventos(event, context):
    """GET /eventos?tipo=&fecha= — asoma el data lake crudo.

    Es la prueba visible de que los eventos llegan a S3. En la demo se abre
    esto despues de completar una tarea y se ve el objeto recien escrito.
    """
    import boto3
    s3 = boto3.client("s3")

    tenant_id = tenant_de(event)
    q = event.get("queryStringParameters") or {}
    fecha = q.get("fecha", datetime.now(timezone.utc).date().isoformat())

    prefijo = f"eventos/tenant_id={tenant_id}/fecha={fecha}/"
    if q.get("tipo"):
        prefijo += f"tipo={q['tipo']}/"

    r = s3.list_objects_v2(Bucket=lago.BUCKET, Prefix=prefijo,
                           MaxKeys=min(int(q.get("limite", 50)), 200))

    objetos = [{
        "clave": o["Key"],
        "tipo": o["Key"].split("tipo=")[1].split("/")[0] if "tipo=" in o["Key"] else None,
        "bytes": o["Size"],
        "escrito_en": o["LastModified"].isoformat(),
    } for o in r.get("Contents", [])]

    return ok(objetos, meta={
        "total": r.get("KeyCount", 0),
        "bucket": lago.BUCKET,
        "prefijo": prefijo,
        "hay_mas": r.get("IsTruncated", False),
    })
