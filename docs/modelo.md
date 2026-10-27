# Modelo de datos — ms-analitico

**Motor:** DynamoDB
**Tabla:** `ms-analitico-analitico-dev` (tabla unica, single-table design)

## Claves

| Clave | Formato | Por que |
|---|---|---|
| `pk` (HASH) | `TENANT#<tenant_id>` | el aislamiento por tenant vive en la particion: una query no puede cruzar tenants |
| `sk` (RANGE) | `<ENTIDAD>#<id>` | permite varias entidades en la misma tabla y listados por prefijo |

## Estructura de los items

> TODO(@Osomar1705): completar con tus entidades reales.

```json
{
  "pk": "TENANT#t-acme",
  "sk": "EJEMPLO#01HX...",
  "tenant_id": "t-acme",
  "nombre": "...",
  "creado_en": "2026-11-05T14:22:01Z"
}
```

## Indices secundarios

| Indice | pk | sk | Para que consulta |
|---|---|---|---|
| | | | |

## Volumen

| Entidad | Registros por tenant |
|---|---|
| | |

**Total por tenant:** 10,000 · **Total del microservicio:** 30,000
