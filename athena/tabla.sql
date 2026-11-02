-- Tabla externa de Athena sobre el data lake de S3.
-- Se crea UNA vez desde la consola de Athena (o con la CLI), reemplazando
-- <BUCKET> por el nombre real del bucket (lo da `sls info` de infra-compartida).

CREATE DATABASE IF NOT EXISTS ppm_analitico;

CREATE EXTERNAL TABLE IF NOT EXISTS ppm_analitico.eventos (
    evento_id            STRING,
    tipo                 STRING,
    origen               STRING,
    ocurrido_en          STRING,
    version              INT,
    proyecto_id          STRING,
    tarea_id             STRING,
    recurso_id           STRING,
    codigo               STRING,
    nombre               STRING,
    portafolio_id        STRING,
    presupuesto_estimado DOUBLE,
    monto                DOUBLE,
    total_gastado        DOUBLE,
    monto_aprobado       DOUBLE,
    ejecutado_pct        DOUBLE,
    total_tareas         INT,
    tareas_completadas   INT,
    avance_pct           DOUBLE,
    horas_semana         DOUBLE,
    costo_hora           DOUBLE,
    ocupacion_pct        DOUBLE,
    prioridad_score      DOUBLE
)
PARTITIONED BY (
    tenant_id STRING,
    fecha     STRING,
    tipo_part STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://<BUCKET>/eventos/';

-- Descubrir las particiones ya escritas (hay que repetirlo cuando entran
-- fechas nuevas):
MSCK REPAIR TABLE ppm_analitico.eventos;

-- ----------------------------------------------------------------- consultas

-- 1. Tareas completadas por mes y por tenant
SELECT   tenant_id, substr(fecha, 1, 7) AS mes, COUNT(*) AS completadas
FROM     ppm_analitico.eventos
WHERE    tipo = 'tarea.completada'
GROUP BY tenant_id, substr(fecha, 1, 7)
ORDER BY mes, tenant_id;

-- 2. Gasto acumulado por tenant
SELECT   tenant_id, SUM(monto) AS gastado, COUNT(*) AS n_gastos
FROM     ppm_analitico.eventos
WHERE    tipo = 'gasto.registrado'
GROUP BY tenant_id;

-- 3. Proyectos que cruzaron el umbral de presupuesto
SELECT   tenant_id, codigo, ejecutado_pct, ocurrido_en
FROM     ppm_analitico.eventos
WHERE    tipo = 'presupuesto.excedido'
ORDER BY ejecutado_pct DESC;

-- 4. Volumen de eventos por tipo (la prueba de que el EDA esta vivo)
SELECT   tipo, COUNT(*) AS n
FROM     ppm_analitico.eventos
GROUP BY tipo
ORDER BY n DESC;
