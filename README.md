# ms-analitico

Data lake de eventos en S3, KPIs del portafolio y las alertas al PMO.
**Es el consumidor universal:** su cola ve todo el tráfico del sistema.

- **Responsable:** @Osomar1705
- **Base de datos:** S3 (data lake) + Athena · DynamoDB para los agregados
- **Runtime:** Python 3.12 · Serverless Framework · us-east-1

## Endpoints (4, todos protegidos)

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/kpis` | el dashboard ejecutivo: avance global, ejecución presupuestal, sobreasignaciones |
| GET | `/kpis/serie` | la evolución mes a mes, para los gráficos |
| GET | `/eventos` | asoma el data lake crudo en S3 · `?tipo=`, `?fecha=` |
| GET | `/health` | verifica el servicio |

`GET /eventos` es la **prueba visible del EDA**: en la demo, después de
completar una tarea, se abre esto y se ve el objeto recién escrito en S3.

## Los dos workers

```
                     SNS ppm-eventos
                      /           \
     (sin filtro)    /             \   (filtro: *.excedido,
                    /               \   *.sobreasignado)
         cola-analitico          cola-alertas
                |                      |
          ingesta.manejar       notificador.manejar
             /      \                  |
        S3 crudo   agregados     SNS ppm-alertas
       (histórico) (dashboard)    (correo al PMO)
```

**`ingesta`** es el único consumidor **sin filter policy**: el data lake tiene
que archivarlo todo. Con cada evento hace dos cosas — lo guarda crudo en S3 y
suma sus contadores en DynamoDB.

**`notificador`** convierte los eventos de riesgo en mensajes legibles:

> `PRESUPUESTO EN RIESGO - t-acme`
> El proyecto PRY-ACM-00042 ejecutó 94.3% de su presupuesto (S/ 848,700 de
> S/ 900,000). Superó el umbral de 90% y quedó marcado en rojo.

Para que lleguen por correo, basta suscribir una dirección al topic
`ppm-alertas` desde la consola — sin tocar código.

## Por qué el dashboard no lee de Athena

Una consulta de Athena tarda segundos y se cobra por byte escaneado. Un panel
que se refresca constantemente no puede depender de eso. Así que S3 guarda el
histórico y DynamoDB mantiene los contadores ya sumados. Es el patrón normal de
un data lake: almacenamiento barato para el histórico, vista materializada para
la lectura en caliente.

Athena sigue ahí para el análisis ad-hoc: [`athena/tabla.sql`](athena/tabla.sql)
trae la tabla externa y cuatro consultas listas para la demo.

## Desplegar

```bash
npm install            # instala Serverless v3 y el plugin de Python
npx serverless deploy
```

Necesita **Docker corriendo** si este microservicio usa RDS: el plugin compila
`psycopg` contra Amazon Linux dentro de un contenedor.

## Cargar datos

```bash
export BUCKET_DATALAKE=$(aws cloudformation describe-stacks \
  --stack-name infra-compartida-dev \
  --query "Stacks[0].Outputs[?OutputKey=='BucketDatalake'].OutputValue" --output text)
export TABLA_AGREGADOS=ms-analitico-agregados-dev
python seed/generar.py            # 30,000 eventos en 3 tenants
python seed/generar.py --contar
```

Después, en Athena: `MSCK REPAIR TABLE ppm_analitico.eventos` para que descubra
las particiones.

## Documentación

- [Modelo de datos](docs/modelo.md) — particionado, agregados, las dos colas
- [Consultas de Athena](athena/tabla.sql)
- [Arquitectura general](https://github.com/ppm-cloud-utec/infra-compartida/blob/main/docs/arquitectura.md)
