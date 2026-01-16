# README — ETL Target Collections (AWS Glue)

## Descripción
Este job de AWS Glue procesa el insumo **Target Collections** en formato **CSV** desde S3, lo transforma (cast + orden de columnas), actualiza la tabla **aux.target_collections** en PostgreSQL y finalmente archiva el archivo procesado en S3 con versionado por fecha.

---

## Flujo del proceso

1. **Lee un único CSV** desde:
   - `s3://{bucket}/{s3_input_prefix}/`
   - Regla: **debe existir exactamente 1 archivo** y debe terminar en `.csv`.

2. **Transforma el DataFrame**
   - Aplica `cast_and_order_target_collections(dfs)` para:
     - Castear tipos (date, string, float, int)
     - Forzar orden de columnas: `date, facility, target, facility_group_id`

3. **Actualiza la tabla en DB**
   - Tabla destino: `aux.target_collections`
   - Ejecuta `update_table(...)` usando credenciales desde Secrets Manager.

4. **Archiva el archivo procesado**
   - Mueve el CSV desde `input_files` hacia `history_files`
   - Añade sufijo de fecha: `_YYYY-MM-DD`
   - Ejemplo:
     - `.../input_files/targets.csv`
     - `.../history_files/targets_2026-01-13.csv`

5. **Notifica por SNS**
   - En éxito: asunto `OK Target collections`
   - En error: asunto `FAIL` con stacktrace en logs

---

## Parámetros requeridos del Job (Glue Args)

El script requiere que se ejecuten estos argumentos:

- `--DB_SECRET_NAME` : nombre del secret en AWS Secrets Manager (credenciales DB)
- `--AWS_REGION`     : región AWS del secret
- `--ENV`            : ambiente (`dev` / `prod`)

Ejemplo:
```bash
--DB_SECRET_NAME ******
--AWS_REGION us-east-2
--ENV dev
