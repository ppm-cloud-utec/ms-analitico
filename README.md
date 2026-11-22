# ms-analitico

Data lake de eventos e indicadores del tablero.

Parte del proyecto final de Cloud Computing (CS2032), Grupo 4. Todas las rutas cuelgan de la API compartida `ppm-api-dev` bajo `/analitico` y, salvo el login, piden el token JWT.

Base de datos: S3 (data lake) y DynamoDB (agregados).

## Endpoints

- GET /analitico/health
- GET /analitico/kpis
- GET /analitico/kpis/serie
- GET /analitico/eventos

## Eventos

Consume todos los eventos: los guarda en el data lake y actualiza los agregados. Las alertas (presupuesto.excedido, recurso.sobreasignado) se reenvian al topic ppm-alertas.

## Despliegue

Requiere que `infra-compartida` ya este desplegado.

```bash
npm install
npx serverless deploy
```

## Datos de prueba

```bash
python seed/generar.py
python seed/generar.py --contar
```
