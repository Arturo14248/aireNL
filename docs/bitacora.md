# Bitácora de AireNL

Registro de cambios y de la lógica detrás de cada decisión. Cada entrada marca lo que **queda por decidir** para que el equipo elija la opción final.

---

## 2026-11-16 · Capa de ingesta: cliente OpenAQ, DAG `ingest_openaq` y ajustes de repositorio

Rama: `feat/ingestion-architecture-definition`. Ningún cambio se ha commiteado todavía.

### Resumen

| # | Cambio | Archivos | Motivo |
| --- | --- | --- | --- |
| 1 | `.gitignore` reescrito | `.gitignore` | Ignoraba archivos que sí deben versionarse |
| 2 | Variable `API_KEY` → `OPENAQ_API_KEY` | `.env.example`, `.env` (local), `scripts/exploration/*.py` | Nombre estándar del SDK y de la arquitectura |
| 3 | Paquete `ingestion/` en la raíz | `ingestion/**`, `pyproject.toml` | Estructura de la sección 9 de la arquitectura |
| 4 | Contrato `Measurement` (Pydantic + Avro) | `ingestion/models.py`, `schemas/measurement.avsc` | Esquema estándar de la sección 5.2 |
| 5 | Cliente reutilizable de OpenAQ | `ingestion/clients/openaq.py` | Requisito de la sección 5.2.1 |
| 6 | Sink temporal en JSON Lines | `ingestion/sinks.py` | Kafka aún no existe |
| 7 | DAG `ingest_openaq` | `orchestration/dags/ingest_openaq.py` | Requisito de las secciones 5.2.1 y 5.7 |
| 8 | Docker Compose alineado con la arquitectura | `docker-compose.yaml` | LocalExecutor, montar el cliente, credencial como Variable |
| 9 | Pruebas unitarias | `ingestion/tests/**` | Sección 5.9: pytest para clientes |

---

### 1. `.gitignore`

**Antes** ignoraba `pyproject.toml`, `.pre-commit-config.yaml` y cualquier ruta llamada `airenl` (lo que excluía `src/airenl/`).

**Cambio:** se dejaron de ignorar esos tres. Se agregaron reglas genéricas: `__pycache__/` en cualquier carpeta (antes solo en `orchestration/dags/`), `.venv/`, cachés de ruff, pytest y mypy, `build/`, `dist/`, `.DS_Store` y `data/` (la zona de aterrizaje local de la ingesta).

**Por qué:**
- `pyproject.toml` define las dependencias y la configuración de ruff y pytest. Sin versionarlo, nadie más (ni CI) puede reproducir el entorno, y `uv.lock` queda huérfano. `uv.lock` **sí debe commitearse**: es lo que garantiza versiones idénticas en cada máquina.
- `.pre-commit-config.yaml` es la configuración compartida de los hooks. Si cada quien tiene la suya, los hooks no sirven como control de calidad común.
- `.env` sigue ignorado: es el único archivo con secretos.
- En `.pre-commit-config.yaml` se excluyó `uv.lock` del hook `check-added-large-files`. El lockfile pesa unos 1.2 MB (el límite es 500 KB), lo genera `uv` y debe versionarse. Se prefirió excluir ese archivo a subir el límite para todos.

**Por decidir:**
- [ ] Si se ignoró `pyproject.toml` a propósito (por ejemplo, para no publicar algo), revertir esa línea. Hoy no contiene secretos.
- [ ] Commitear `uv.lock`, `pyproject.toml` y `.pre-commit-config.yaml` en este PR.

### 2. `OPENAQ_API_KEY`

**Cambio:** `.env.example` y los dos scripts de exploración usan `OPENAQ_API_KEY`. En tu `.env` local se renombró solo la línea `API_KEY=` → `OPENAQ_API_KEY=`; el valor no se tocó. También se agregaron a `.env.example` las dos variables del usuario web de Airflow que ya tenías en `.env`.

**Por qué:** es el nombre que usa la arquitectura (sección 9) y el que el SDK `openaq` lee automáticamente. Un nombre genérico como `API_KEY` choca en cuanto haya otra API (Open-Meteo, la API de ingesta con `INGEST_API_KEY`).

Nota: el error de fechas en `scripts/exploration/measurements.py` (pasar fechas con hora junto con `data="days"`) ya lo habías corregido tú mientras se hacían estos cambios; no se tocó esa línea.

### 3. Estructura del paquete: `ingestion/` en la raíz

**Cambio:** el código vive en `ingestion/` (como en la sección 9 de la arquitectura) y `pyproject.toml` ahora empaqueta `ingestion*` desde la raíz (excluyendo las pruebas). `testpaths` apunta a `ingestion/tests`.

**Por qué:** la arquitectura organiza el repo por componente (`api/`, `ingestion/`, `streaming/`, `analytics/`). Cada carpeta se monta en su contenedor. Con el layout `src/airenl/` habría que mantener dos convenciones.

**Por decidir:**
- [ ] `src/airenl/__init__.py` (vacío) queda huérfano: ya no se empaqueta. Recomendación: borrarlo. No se borró para que lo decidas tú.
- [ ] Alternativa: mantener el layout `src/` (`src/airenl/ingestion/...`). Es más robusto contra imports accidentales, pero se aleja del documento de arquitectura.

### 4. Contrato `Measurement`

**Cambio:** `ingestion/models.py` (Pydantic) y `schemas/measurement.avsc` (Avro) describen el mismo registro. Una prueba verifica que coincidan los campos, su orden y su nulabilidad, y que el ejemplo JSON de la sección 5.2.2 siga siendo válido.

Campos de la arquitectura: `station_id`, `sensor_id`, `parameter`, `value`, `unit`, `measured_at`, `source`.

Decisiones dentro del contrato:

| Decisión | Por qué | Alternativa |
| --- | --- | --- |
| `measured_at` = **fin** de la hora | OpenAQ filtra `/hours` por el fin del periodo (verificado: la ventana `(12:00, 13:00]` devuelve la hora 12:00–13:00). Además, las redes mexicanas reportan promedios horarios con hora final. | Inicio de la hora: también funciona, pero hay que convertirlo en cada consulta. |
| `value` admite `null` | Silver debe recibir los valores nulos para mandarlos a `aq.dlq` con su motivo. El simulador inyecta nulos a propósito. | Descartarlos en la ingesta, pero se perderían sin rastro. |
| Campos extra opcionales: `period_start`, `coverage_pct`, `ingested_at` | `coverage_pct` sirve para el criterio de suficiencia de datos de la norma (`int_daily_averages`). `ingested_at` sirve para medir la latencia de extremo a extremo (sección 5.9). | Dejar solo los 7 campos del documento. Al ser opcionales, no afectan a la API push. |
| IDs con prefijo de fuente: `openaq-4408711`, `openaq-13051191` | Evita colisiones con SINAICA, el simulador y socios, y deja claro de dónde viene cada registro. | IDs propios del proyecto (`nl-obispado`), mapeados desde `stations.csv`. |
| Unidades en ASCII sin convertir (`ug/m3`, `ppm`) | La conversión de unidades es tarea de silver según la arquitectura. | Convertir en la ingesta. |

**Por decidir:**
- [ ] Confirmar la convención de hora final.
- [ ] Mantener o quitar los tres campos extra.
- [ ] Prefijos de fuente o IDs propios.

### 5. Cliente `ingestion/clients/openaq.py`

**Qué hace:**
- `find_stations()` descubre las estaciones del AMM con un bounding box (`AMM_BBOX = (-100.75, 25.40, -99.90, 26.00)`) y se queda con las de **SINAICA** (`provider 223`) marcadas como monitor, que reportaron en los últimos 30 días, y solo los contaminantes con NOM (`pm25`, `pm10`, `o3`, `no2`, `so2`, `co`).
- `iter_hours()` / `fetch_measurements()` piden promedios horarios por sensor en la ventana `(inicio, fin]` y los traducen a `Measurement`.
- Paginación automática, espaciado de solicitudes (1.1 s, menos de 60/min), reintentos con espera exponencial y jitter ante 429, 5xx y errores de red, respeto de `Retry-After` y `x-ratelimit-*`. Los errores 401/403 no se reintentan.
- Si un sensor falla tras agotar los reintentos, se registra en `failed_sensors` y se siguen pidiendo los demás.

**Verificado contra la API real (2026-11-16):** encontró las 15 estaciones activas de SIMA (Juárez, Apodaca, UANL, Pesquería, San Pedro, Obispado, San Bernabé, Escobedo, ITESM, Cadereyta, García, San Nicolás, ITNL, Misión San Juan y Santa Catarina), con 90 sensores. Para 3 estaciones y 24 h: 19 solicitudes, 20 s y 282 registros de 432 posibles (la fuente tiene huecos).

| Decisión | Por qué | Alternativa |
| --- | --- | --- |
| `httpx` directo, sin el SDK `openaq` | La arquitectura lo pide (sección 5.2.1). `httpx` y `pydantic` ya vienen en la imagen de Airflow, así que no hay que instalar nada en los contenedores. Además da control total de reintentos y del límite de solicitudes. | El SDK: habría que instalarlo en la imagen y no controla bien los reintentos. |
| Bounding box en lugar de radio de 12 km | El radio solo encontraba unas 7 de las 15 estaciones activas. | Radio mayor: un círculo cubre mal la forma del AMM. |
| Solo SINAICA | Es la red oficial y la única válida para comparar con la NOM. Filtrar por fecha del último dato también elimina los duplicados (IDs viejos de AirNow y de SINAICA). | Incluir AirGradient (sensores de bajo costo, casi en tiempo real). Encaja mejor por la vía push o como una fuente separada con `source` distinto. |
| Sin `no` ni `nox` | No tienen límite en la NOM. Excluirlos ahorra 30 solicitudes por corrida. | Incluirlos para análisis de química atmosférica: basta con pasar `parameters=None`. |
| Descubrir estaciones en cada corrida | Cuesta 1 solicitud y detecta estaciones nuevas o dadas de baja sin intervención. | Leer de `stations.csv`: más estable y auditable. Se puede generar desde los snapshots que guarda el DAG. |
| Continuar si falla un sensor | Un sensor roto no debe bloquear a toda la metrópoli. | Fallar al primer error (`skip_failed_sensors=False`). |

**Por decidir:**
- [ ] Incluir AirGradient y/o `no`/`nox`.
- [ ] Descubrimiento dinámico o seed fija de estaciones.

### 6. Sink temporal: JSON Lines

**Cambio:** `ingestion/sinks.py` escribe un archivo por ventana en `data/landing/openaq/measurements/window_end=YYYY-MM-DDTHH/part-0.jsonl`. La escritura es atómica (archivo temporal y luego rename) y sobrescribe al re-correr.

**Por qué:** Kafka y Apicurio todavía no están en el compose. Esto permite probar la ingesta completa hoy, y cuando exista el perfil `core` solo se cambia el sink por un productor a `aq.measurements.raw` con llave `station_id`. El cliente no cambia.

**Por decidir:**
- [ ] Mantener este sink al llegar Kafka (como respaldo y para depurar) o eliminarlo.

### 7. DAG `ingest_openaq`

`discover_stations` → `extract_measurements`. Por XCom solo pasa la lista de estaciones (pequeña). `discover_stations` también guarda un snapshot diario del catálogo en `data/landing/openaq/stations/`.

| Decisión | Por qué | Alternativa |
| --- | --- | --- |
| **Cada hora** (`0 * * * *`, UTC), no cada 15 min | Las estaciones reportan una vez por hora (sección 11), así que tres de cada cuatro corridas no traerían nada nuevo. | Cada 15 min, como dice la sección 5.2.1. El cliente lo soporta; solo cambia el horario. |
| Ventana `(data_interval_end − lookback_hours, data_interval_end]`, con **lookback de 120 h** (parámetro de 1 a 720) | **Hallazgo clave:** los datos de SINAICA llegan a OpenAQ con unos 4 días de retraso (el último dato de todas las estaciones era del 2026-11-12 16:00Z). Pidiendo solo "la última hora" casi nunca llegaría nada. | Ventana exacta sin traslape (lo que dice la sección 5.2.1). Solo es viable si el retraso desaparece o si la fuente principal pasa a ser SINAICA directo. |
| Traslape permitido | Volver a pedir horas repetidas es seguro: silver deduplica por `sensor_id` + `measured_at`, y cada corrida sobrescribe su propio archivo. Costo: ~91 solicitudes por hora, frente a un límite de 2,000/h. | — |
| `CronDataIntervalTimetable` explícito | En Airflow 3, un `schedule` cron simple ya no genera intervalos de datos (`data_interval_start == end`). Esto rompería el patrón de ventana y el backfill. | `timedelta(hours=1)`. |
| `catchup=False`, `max_active_runs=1`, 3 reintentos con backoff, timeout de 30 min | El lookback ya cubre los huecos recientes. Para periodos viejos se usa un backfill manual. | — |
| Si fallan sensores, la tarea falla **después** de escribir | Los datos sanos ya quedan guardados, el fallo se ve en la UI y el reintento reescribe el mismo archivo. | Tarea en verde con advertencia en el log, aunque eso esconde el problema. |
| API key vía `Variable.get("openaq_api_key")` | La sección 5.7 pide credenciales en Variables o Conexiones de Airflow, no en el código. | Una Conexión HTTP de Airflow con la key en `password`. |

**Por decidir:**
- [ ] Horario de 1 h o de 15 min.
- [ ] Valor del lookback (120 h cubre el retraso actual con margen). Conviene evaluar si SINAICA directo es más oportuno y usarlo como fuente en vivo.
- [ ] Actualizar la sección 5.2.1 de `architecture.md` con lo que se decida (no se modificó).

### 8. Docker Compose

| Cambio | Por qué |
| --- | --- |
| `CeleryExecutor` → `LocalExecutor`. Se eliminaron `redis`, `airflow-worker` y `flower`, y las variables `AIRFLOW__CELERY__*`. | La sección 5.7 define LocalExecutor. En una laptop con 16 GB, Redis y un worker aparte consumen memoria sin aportar nada a esta escala. |
| `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` | Los DAGs de ejemplo saturan la UI y el procesador de DAGs. Si el stack ya se había levantado con ejemplos, hay que limpiarlos de la base (ver la entrada "Nota: DAGs de ejemplo huérfanos" más abajo). |
| Montaje `./ingestion → /opt/airflow/project/ingestion` (solo lectura) y `PYTHONPATH=/opt/airflow/project` | Hace que el cliente sea importable desde los DAGs sin construir una imagen propia. Se monta en una subcarpeta y no en `/opt/airflow` para no exponer `logs/`, `config/`, etc. como paquetes de Python. |
| Montaje `./data → /opt/airflow/data` y `AIRENL_LANDING_DIR` | Zona de aterrizaje visible desde la Mac. `airflow-init` la crea y le asigna permisos. |
| `AIRFLOW_VAR_OPENAQ_API_KEY: ${OPENAQ_API_KEY:-}` | Expone la key de `.env` como Variable de Airflow sin guardarla en la base de datos. |

**No se cambió, pero conviene decidir:**
- [ ] `platform: linux/amd64`: en Apple Silicon fuerza emulación (más lento y con más memoria). La imagen oficial es multi-arquitectura. Recomendación: quitarlo, salvo que lo hayas agregado por un problema concreto.
- [ ] Perfiles de compose (`profiles: [orchestration]`, sección 8): agregarlos cambia la forma de levantar el stack (`docker compose --profile orchestration up`). Conviene hacerlo junto con el `Makefile`.
- [ ] El archivo se llama `docker-compose.yaml`; el documento dice `docker-compose.yml`.
- [ ] Después de cambiar el executor, hay que recrear los contenedores: `docker compose down && docker compose up -d`. El volumen de Postgres se conserva.

### 9. Pruebas

`uv run pytest`: 16 pruebas, sin red. Cubren paginación, reintentos 429/503/red, que 401 no se reintente, el filtrado de estaciones (inactivas, otros proveedores, sin contaminantes NOM), los bordes de la ventana `(inicio, fin]`, el mapeo a `Measurement`, los sensores fallidos, la escritura atómica y que el contrato Avro y el Pydantic coincidan. `ruff check` y `ruff format` pasan.

**No verificado:** el DAG no se ejecutó dentro de Airflow porque Docker no estaba corriendo. Pasos para validarlo:

```bash
docker compose down && docker compose up -d
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test ingest_openaq
ls data/landing/openaq/measurements/
```

---

## 2026-11-16 · Fix: `extract_measurements` fallaba con "datetime must be timezone-aware"

**Síntoma:** en la primera corrida con Docker, `discover_stations` terminó bien (se escribió `stations.json`), pero `extract_measurements` falló antes de pedir cualquier medición:

```
ValueError: datetime must be timezone-aware, got DateTime(2026, 11, 12, 3, 0, 0)
```

**Causa raíz (reproducida dentro del contenedor, Python 3.13, pendulum 3.2.0):** Airflow pone en el contexto `data_interval_end` como un `pendulum.DateTime`. El DAG hacía `end.astimezone(timezone.utc)`, que devuelve un `pendulum.DateTime` con `tzinfo` de la librería estándar. Al restarle el lookback (`window_end - timedelta(hours=120)`), pendulum **pierde la zona horaria** y devuelve una fecha naive:

```python
e = pendulum.datetime(2026, 11, 17, 3, tz="UTC")
a = e.astimezone(timezone.utc)   # DateTime(..., tzinfo=datetime.timezone.utc)
a - timedelta(hours=120)         # DateTime(2026, 11, 12, 3, 0, 0)  <- naive
```

El cliente rechaza fechas naive a propósito (una fecha sin zona se interpretaría como UTC sin serlo y movería la ventana 6 h), así que la tarea falló. Era un error del DAG, no de la API.

**Cambio:**
- Nueva función pública `to_utc()` en `ingestion/clients/openaq.py`: rechaza fechas naive y reconstruye el valor como un `datetime` **de la librería estándar** en UTC, para que ningún subtipo (pendulum) llegue a la aritmética de fechas.
- El DAG (`_window_end`) usa `to_utc()` antes de restar el lookback.
- `iter_hours()` y `fetch_measurements()` normalizan con `to_utc()` todas las fechas que reciben, así que el cliente también acepta directamente los valores de pendulum de Airflow.
- Dos pruebas de regresión (`pendulum` agregado al grupo `dev` de dependencias). Total: 18 pruebas en verde.

**Limpieza:** se borró `src/airenl.egg-info`. Eran metadatos obsoletos del layout `src/` (4 de noviembre; empaquetaba `airenl`). El vigente es `airenl.egg-info` en la raíz, que regenera `uv sync` y empaqueta `ingestion`. Ambos son artefactos de build ignorados por git.

**Verificado (2026-11-16):** `airflow dags test ingest_openaq` dentro del contenedor terminó bien y escribió tanto `stations.json` como las mediciones en `data/landing/openaq/measurements/`.

---

## 2026-11-16 · Nota: DAGs de ejemplo huérfanos tras desactivar `LOAD_EXAMPLES`

**Síntoma:** `airflow dags list` falla con:

```
airflow.serialization.helpers.TimetableNotRegistered: Timetable class
'airflow.example_dags.plugins.workday.AfterWorkdayTimetable' is not registered ...
```

`airflow dags list-import-errors` responde `No data found` e `ingest_openaq` corre con normalidad.

**Causa:** con la configuración anterior (`LOAD_EXAMPLES: 'true'`), Airflow serializó sus DAGs de ejemplo en Postgres. Uno de ellos, `example_workday_timetable`, usa un timetable personalizado que solo existe mientras se cargan los plugins de ejemplo. Al desactivar los ejemplos, esa clase ya no se registra, pero la fila del DAG sigue en la base porque `docker compose down` conserva el volumen de Postgres. `dags list` intenta deserializar todos los DAGs guardados y falla en ese. El scheduler y la ejecución de `ingest_openaq` no se ven afectados.

**Afecta a:** cualquier persona que haya levantado el stack con la versión anterior del `docker-compose.yaml`. Una instalación nueva no lo presenta.

**Solución (elegir una):**

```bash
# A) Recomendada: borrar solo el DAG de ejemplo problemático (conserva historial y Variables de la UI)
docker compose exec airflow-scheduler airflow dags delete example_workday_timetable -y

# B) Reiniciar la base de Airflow por completo (borra historial de corridas y lo creado desde la UI;
#    la API key no se pierde porque viene de .env)
docker compose down -v --remove-orphans
docker compose up -d
```

Los demás DAGs de ejemplo quedan como inactivos y son inofensivos; se pueden borrar igual con `airflow dags delete <dag_id> -y`.
