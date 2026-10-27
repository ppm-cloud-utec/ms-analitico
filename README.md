# ms-analitico

Data lake de eventos en S3, KPIs del portafolio con Athena

- **Responsable:** @Osomar1705
- **Base de datos:** S3 (data lake) + Athena
- **Runtime:** Python 3.12 · Serverless Framework v4 · us-east-1

## Desplegar

```bash
pip install -r requirements.txt
npm install
sls deploy
```

`infra-compartida` tiene que estar desplegado antes: de ahi salen el topic SNS,
el authorizer y (si aplica) el endpoint del RDS.

## Endpoints

| Metodo | Ruta | Que hace |
|---|---|---|
| GET | `/health` | verifica el servicio y devuelve el tenant del token |

> TODO(@Osomar1705): completar con tus 5+ endpoints.

## Probar

```bash
# sin token -> 401 (esta es la captura que pide el informe)
curl -i $URL/health

# con token
TOKEN=$(curl -s -X POST $URL_IDENTIDAD/login \
  -d '{"tenant_id":"t-acme","email":"pmo@acme.com","password":"..."}' | jq -r .datos.token)
curl -s $URL/health -H "Authorization: Bearer $TOKEN" | jq
```

## Datos

30,000 registros ficticios en 3 tenants (`t-acme`, `t-globex`, `t-initech`).

```bash
python seed/generar.py
```

## Documentacion

- [Modelo de datos](docs/modelo.md)
- [Arquitectura general](https://github.com/ppm-cloud-utec/infra-compartida/blob/main/docs/arquitectura.md)
