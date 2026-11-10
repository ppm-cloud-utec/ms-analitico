# Modelo de datos — ms-analitico

Este microservicio es distinto a los otros cinco: **su base de datos es S3**
(un data lake de eventos), consultable con Athena, más una tabla pequeña de
agregados en DynamoDB para el dashboard.

## Por qué dos almacenes y no uno

**S3 es la fuente de verdad.** Cada evento del sistema se archiva crudo, y de
ahí sale el análisis histórico con Athena.

**DynamoDB es la vista rápida.** El dashboard ejecutivo no puede leer de Athena:
una consulta tarda segundos y se cobra por byte escaneado, y un panel que se
refresca constantemente lo haría inviable. Así que el worker de ingesta, con el
mismo evento, hace dos cosas: lo guarda en S3 **y** suma sus contadores en
DynamoDB.

Es el patrón normal de un data lake: almacenamiento barato para el histórico,
vista materializada para la lectura en caliente.

## El data lake en S3

### Particionado

```
s3://ppm-datalake-<cuenta>-dev/
└── eventos/
    └── tenant_id=t-acme/
        └── fecha=2026-11-01/
            └── tipo=tarea.completada/
                └── 3f8a....json
```

Particionar por `tenant_id`, `fecha` y `tipo` es lo que hace que Athena
**escanee solo lo consultado** en vez del bucket entero. Una consulta de las
tareas completadas de Acme en noviembre toca tres particiones, no 30,000
objetos.

El `tenant_id` como primera partición también mantiene el aislamiento: una
consulta de un tenant nunca toca los archivos de otro.

### Forma del objeto

Los eventos se guardan **aplanados**, en una sola capa:

```json
{
  "evento_id": "3f8a...",
  "tipo": "tarea.completada",
  "tenant_id": "t-acme",
  "origen": "ms-tareas",
  "ocurrido_en": "2026-11-01T14:22:01+00:00",
  "version": 1,
  "proyecto_id": "7",
  "tarea_id": "9c84...",
  "total_tareas": 20,
  "tareas_completadas": 13,
  "avance_pct": 65.0
}
```

**Por qué aplanado:** Athena sobre JSON anidado es incómodo, y los campos de
`datos` cambian según el tipo de evento. Al aplanarlo, la tabla externa declara
todas las columnas posibles y las que no aplican quedan en `null`.

## La tabla de agregados (DynamoDB)

| Clave | Formato |
|---|---|
| `pk` | `TENANT#<tenant_id>` |
| `sk` | `PERIODO#2026-11` (mes) o `PERIODO#TOTAL` (acumulado) |

```json
{
  "pk": "TENANT#t-acme",
  "sk": "PERIODO#TOTAL",
  "eventos": 10000,
  "proyectos_creados": 500,
  "tareas_creadas": 3400,
  "tareas_completadas": 3000,
  "gastos": 1700,
  "monto_gastado": 48392011.55,
  "presupuesto_comprometido": 271540000,
  "asignaciones": 900,
  "horas_comprometidas": 20880,
  "sobreasignaciones": 90,
  "presupuestos_excedidos": 110,
  "repriorizaciones": 300
}
```

Los contadores se suman con `ADD`, que es **atómico**: dos lotes de SQS
procesándose a la vez no se pisan. El worker escribe en **dos periodos** por
evento: el mes (para la serie de tiempo de los gráficos) y `TOTAL` (para las
tarjetas del dashboard).

## Tabla externa de Athena

Está en [`athena/tabla.sql`](../athena/tabla.sql), junto con cuatro consultas
listas para la demo. Se crea una vez desde la consola de Athena reemplazando
`<BUCKET>`, y hay que correr `MSCK REPAIR TABLE` cuando entran fechas nuevas.

## Las dos colas

| Cola | Filtro | Worker | Qué hace |
|---|---|---|---|
| `ppm-cola-analitico` | **ninguno** | `ingesta.manejar` | archiva en S3 y suma agregados |
| `ppm-cola-alertas` | `presupuesto.excedido`, `recurso.sobreasignado` | `notificador.manejar` | publica un mensaje legible en el topic `ppm-alertas` |

La cola de ingesta es la **única sin filter policy**: el data lake tiene que
archivarlo todo. Las dos tienen DLQ con `maxReceiveCount: 3`.

**Detalle de idempotencia:** el mismo evento se archiva *y* se notifica, que son
dos efectos independientes. Por eso el notificador usa la clave
`alerta:<evento_id>` en lugar del `evento_id` desnudo — si usaran la misma, el
que corriera segundo creería que ya está hecho y no haría su trabajo.

## Volumen de datos

| Tenant | Eventos |
|---|---|
| `t-acme` | 10,000 |
| `t-globex` | 10,000 |
| `t-initech` | 10,000 |
| **Total** | **30,000** |

Distribuidos en los últimos 12 meses, con la proporción que tiene el tráfico
real del sistema:

| Tipo | Por tenant |
|---|---|
| `tarea.creada` | ~3,400 |
| `tarea.completada` | ~3,000 |
| `gasto.registrado` | ~1,700 |
| `recurso.asignado` | ~900 |
| `proyecto.creado` | ~500 |
| `proyecto.priorizado` | ~300 |
| `presupuesto.excedido` | ~110 |
| `recurso.sobreasignado` | ~90 |

Hay muchas más tareas que proyectos y muy pocas alertas, que es como se ve el
tráfico de un sistema real.

> **Nota sobre el seed.** Hacer 30,000 `PutObject` tomaría horas. El seed agrupa
> los eventos de cada partición en un objeto **JSON Lines** (muchos eventos, un
> archivo), que Athena lee igual de bien, y los sube con 32 hilos en paralelo:
> ~2,000 objetos por tenant en menos de un minuto.
>
> **El seed borra el prefijo del tenant antes de escribir.** La clave lleva el
> número de eventos de la partición (`historico-NNNN.json`), así que dos
> corridas que repartan distinto dejarían los dos objetos y el tenant acabaría
> con más eventos de los que dice tener — pasó: Acme llegó a acumular 16,556 en
> vez de 10,000. Borrar primero es lo que garantiza el conteo exacto.
>
> Los eventos que llegan en vivo durante la demo sí se escriben de a uno, para
> que se vean apareciendo.

## Cargar los datos

```bash
export BUCKET_DATALAKE=$(aws cloudformation describe-stacks \
  --stack-name infra-compartida-dev \
  --query "Stacks[0].Outputs[?OutputKey=='BucketDatalake'].OutputValue" --output text)
export TABLA_AGREGADOS=ms-analitico-agregados-dev

python seed/generar.py            # 30,000 eventos
python seed/generar.py --contar
```
