#!/usr/bin/env python3
"""Carga 30,000 eventos historicos en el data lake de S3 y sus agregados.

    python seed/generar.py            # los 3 tenants
    python seed/generar.py --contar

Reparto: 10,000 eventos por tenant, de los 8 tipos del catalogo, distribuidos
en los ultimos 12 meses.

A diferencia de los otros microservicios, aqui los "registros" son objetos en
S3. Para no hacer 30,000 PutObject (que tomaria horas), los eventos se agrupan
en archivos JSON Lines por particion: un objeto por tenant/fecha/tipo con
muchos eventos dentro. Athena lee JSON Lines igual de bien.

IMPORTANTE: el seed BORRA el prefijo del tenant antes de escribir. La clave
lleva el numero de eventos de la particion (historico-NNNN.json), asi que si
dos corridas reparten distinto quedarian los dos objetos y el tenant acabaria
con mas eventos de los que dice tener. Borrar primero es lo que garantiza que
el conteo final sea exactamente 10,000 por tenant.
"""
import os
import sys
import json
import uuid
import random
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3

BUCKET = os.environ.get("BUCKET_DATALAKE")
TABLA = os.environ.get("TABLA_AGREGADOS", "ms-analitico-agregados-dev")
REGION = os.environ.get("AWS_REGION", "us-east-1")

TENANTS = {
    "t-acme":    dict(nombre="Acme Ingenieria",   proyectos=500, monto=(200_000, 900_000)),
    "t-globex":  dict(nombre="Globex Consulting", proyectos=250, monto=(20_000, 150_000)),
    "t-initech": dict(nombre="Initech Software",  proyectos=25,  monto=(10_000, 60_000)),
}

# Proporcion de cada tipo: hay muchas mas tareas que proyectos, y muy pocas
# alertas. Es la forma que tiene el trafico real del sistema.
MEZCLA = {
    "tarea.creada":          0.34,
    "tarea.completada":      0.30,
    "gasto.registrado":      0.17,
    "recurso.asignado":      0.09,
    "proyecto.creado":       0.05,
    "proyecto.priorizado":   0.03,
    "presupuesto.excedido":  0.011,
    "recurso.sobreasignado": 0.009,
}

EVENTOS_POR_TENANT = 10_000
ORIGEN = {
    "tarea.creada": "ms-tareas", "tarea.completada": "ms-tareas",
    "gasto.registrado": "ms-presupuestos", "presupuesto.excedido": "ms-presupuestos",
    "recurso.asignado": "ms-recursos", "recurso.sobreasignado": "ms-recursos",
    "proyecto.creado": "ms-portafolios", "proyecto.priorizado": "ms-portafolios",
}


def datos_de(tipo, perfil):
    pid = random.randint(1, perfil["proyectos"])
    monto = random.randint(*perfil["monto"])
    if tipo == "proyecto.creado":
        return {"proyecto_id": str(pid), "codigo": f"PRY-{pid:05d}",
                "nombre": f"Proyecto {pid}", "portafolio_id": str(random.randint(1, 20)),
                "presupuesto_estimado": monto}
    if tipo == "proyecto.priorizado":
        return {"proyecto_id": str(pid), "prioridad_score": round(random.uniform(10, 98), 2)}
    if tipo in ("tarea.creada", "tarea.completada"):
        total = random.randint(8, 400)
        return {"proyecto_id": str(pid), "tarea_id": str(uuid.uuid4()),
                "total_tareas": total,
                "tareas_completadas": random.randint(0, total),
                "avance_pct": round(random.uniform(0, 100), 2)}
    if tipo == "gasto.registrado":
        gastado = round(monto * random.uniform(0.05, 1.1), 2)
        return {"proyecto_id": str(pid), "monto": round(monto * random.uniform(0.01, 0.2), 2),
                "total_gastado": gastado, "monto_aprobado": monto,
                "ejecutado_pct": round(100 * gastado / monto, 2)}
    if tipo == "presupuesto.excedido":
        gastado = round(monto * random.uniform(0.9, 1.3), 2)
        return {"proyecto_id": str(pid), "codigo": f"PRY-{pid:05d}",
                "monto_aprobado": monto, "total_gastado": gastado,
                "ejecutado_pct": round(100 * gastado / monto, 2), "umbral_pct": 90}
    if tipo == "recurso.asignado":
        return {"proyecto_id": str(pid), "recurso_id": str(uuid.uuid4()),
                "horas_semana": random.choice([8, 16, 20, 30, 40]),
                "costo_hora": random.choice([25, 50, 90, 140]),
                "semanas": random.randint(2, 24)}
    return {"recurso_id": str(uuid.uuid4()), "nombre": "Persona sobreasignada",
            "ocupacion_pct": random.choice([110, 130, 150, 180]),
            "exceso_pct": random.choice([10, 30, 50, 80]),
            "proyecto_id": str(pid)}


def aplanar(evento):
    plano = {k: evento[k] for k in
             ("evento_id", "tipo", "tenant_id", "origen", "ocurrido_en", "version")}
    for k, v in evento["datos"].items():
        plano[k] = v if not isinstance(v, (dict, list)) else json.dumps(v)
    return plano


def limpiar(tenant_id, s3):
    """Borra los objetos del tenant. Sin esto una recarga acumula duplicados."""
    prefijo = f"eventos/tenant_id={tenant_id}/"
    borrados, token = 0, None
    while True:
        args = {"Bucket": BUCKET, "Prefix": prefijo, "MaxKeys": 1000}
        if token:
            args["ContinuationToken"] = token
        r = s3.list_objects_v2(**args)
        claves = [{"Key": o["Key"]} for o in r.get("Contents", [])]
        if claves:
            # delete_objects borra hasta 1000 por llamada
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": claves})
            borrados += len(claves)
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    return borrados


def generar(tenant_id, perfil, s3, tabla):
    hoy = date.today()
    tipos = list(MEZCLA)
    pesos = [MEZCLA[t] for t in tipos]

    particiones = defaultdict(list)
    contadores = defaultdict(lambda: defaultdict(float))

    for _ in range(EVENTOS_POR_TENANT):
        tipo = random.choices(tipos, weights=pesos)[0]
        cuando = hoy - timedelta(days=random.randint(0, 364))
        datos = datos_de(tipo, perfil)
        evento = {
            "evento_id": str(uuid.uuid4()), "tipo": tipo, "version": 1,
            "tenant_id": tenant_id, "origen": ORIGEN[tipo],
            "ocurrido_en": f"{cuando.isoformat()}T"
                           f"{random.randint(8,20):02d}:{random.randint(0,59):02d}:00+00:00",
            "datos": datos,
        }
        particiones[(cuando.isoformat(), tipo)].append(aplanar(evento))

        # Mismos contadores que suma el worker de ingesta
        mes = cuando.isoformat()[:7]
        for periodo in (mes, "TOTAL"):
            c = contadores[periodo]
            c["eventos"] += 1
            if tipo == "proyecto.creado":
                c["proyectos_creados"] += 1
                c["presupuesto_comprometido"] += datos["presupuesto_estimado"]
            elif tipo == "proyecto.priorizado":
                c["repriorizaciones"] += 1
            elif tipo == "tarea.creada":
                c["tareas_creadas"] += 1
            elif tipo == "tarea.completada":
                c["tareas_completadas"] += 1
            elif tipo == "recurso.asignado":
                c["asignaciones"] += 1
                c["horas_comprometidas"] += datos["horas_semana"]
            elif tipo == "recurso.sobreasignado":
                c["sobreasignaciones"] += 1
            elif tipo == "gasto.registrado":
                c["gastos"] += 1
                c["monto_gastado"] += datos["monto"]
            elif tipo == "presupuesto.excedido":
                c["presupuestos_excedidos"] += 1

    # Un objeto JSON Lines por particion, subidos EN PARALELO.
    # De a uno, 3,000 PutObject sobre la red tardan varios minutos y la carga
    # se queda a medias si algo la interrumpe.
    def subir(item):
        (fecha, tipo), eventos = item
        clave = (f"eventos/tenant_id={tenant_id}/fecha={fecha}/tipo={tipo}/"
                 f"historico-{len(eventos):04d}.json")
        cuerpo = "\n".join(json.dumps(e, ensure_ascii=False) for e in eventos) + "\n"
        s3.put_object(Bucket=BUCKET, Key=clave, Body=cuerpo.encode(),
                      ContentType="application/json")

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(subir, particiones.items()))
    print(f"    {len(particiones)} objetos en S3 con {EVENTOS_POR_TENANT} eventos")

    for periodo, c in contadores.items():
        tabla.put_item(Item={
            "pk": f"TENANT#{tenant_id}", "sk": f"PERIODO#{periodo}",
            **{k: Decimal(str(round(v, 2))) for k, v in c.items()},
        })
    print(f"    {len(contadores)} periodos de agregados")
    return EVENTOS_POR_TENANT


def contar(s3, tabla):
    print(f"Bucket: {BUCKET}\n")
    gran_total = 0
    for tenant_id, perfil in TENANTS.items():
        objetos = eventos = 0
        token = None
        while True:
            args = {"Bucket": BUCKET, "Prefix": f"eventos/tenant_id={tenant_id}/"}
            if token:
                args["ContinuationToken"] = token
            r = s3.list_objects_v2(**args)
            for o in r.get("Contents", []):
                objetos += 1
                # El nombre lleva la cantidad: historico-NNNN.json
                nombre = o["Key"].rsplit("/", 1)[-1]
                if nombre.startswith("historico-"):
                    eventos += int(nombre.split("-")[1].split(".")[0])
                else:
                    eventos += 1
            if not r.get("IsTruncated"):
                break
            token = r["NextContinuationToken"]

        item = tabla.get_item(Key={"pk": f"TENANT#{tenant_id}",
                                   "sk": "PERIODO#TOTAL"}).get("Item") or {}
        print(f"  {perfil['nombre']:<22} {objetos:>5} objetos · "
              f"{eventos:>6} eventos · agregado dice {int(item.get('eventos', 0)):>6}")
        gran_total += eventos
    print(f"\n  TOTAL {gran_total:>6} eventos en el data lake")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contar", action="store_true")
    ap.add_argument("--tenant", choices=list(TENANTS))
    args = ap.parse_args()

    if not BUCKET:
        sys.exit("Falta BUCKET_DATALAKE (sale de `sls info` de infra-compartida)")

    s3 = boto3.client("s3", region_name=REGION)
    tabla = boto3.resource("dynamodb", region_name=REGION).Table(TABLA)

    if args.contar:
        return contar(s3, tabla)

    random.seed(2026)
    objetivo = {args.tenant: TENANTS[args.tenant]} if args.tenant else TENANTS
    total = 0
    print(f"Bucket: {BUCKET}\nTabla:  {TABLA}\n")
    for tenant_id, perfil in objetivo.items():
        print(f"== {perfil['nombre']} ({tenant_id}) ==")
        n = limpiar(tenant_id, s3)
        if n:
            print(f"    {n} objetos previos borrados")
        total += generar(tenant_id, perfil, s3, tabla)
        print()
    print(f"Listo: {total} eventos. Verifica con --contar")
    print("Despues corre MSCK REPAIR TABLE en Athena (ver athena/tabla.sql)")


if __name__ == "__main__":
    main()
