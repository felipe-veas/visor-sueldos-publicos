# Visor de Sueldos Públicos de Chile 🇨🇱

Este repositorio contiene herramientas Python para descargar, unificar, auditar y visualizar datos de Transparencia Activa de instituciones públicas chilenas (Actualmente Consejo para la Transparencia y Senado de la República).

El sistema utiliza DuckDB y Parquet para consultar más de 28GB de datos en milisegundos. Está diseñado para ejecutarse sin infraestructura backend dedicada, haciendo consultas HTTP Range sobre archivos estáticos.

## Características Principales

- 📥 **Smart Sync**: Consulta los servidores de transparencia y descarga archivos solo cuando los headers remotos indican cambios, reduciendo el uso de ancho de banda. Extractor especializado para la API del Senado.
- ⚡️ **Motor de Consultas**: Convierte 28GB de CSVs y respuestas JSON crudas a archivos `.parquet` con compresión ZSTD. Utiliza DuckDB para consultas en memoria.
- ☁️ **Despliegue Stateless**: Arquitectura diseñada para entornos contenerizados Serverless. Utiliza la extensión `httpfs` de DuckDB para lectura remota de archivos directamente desde GitHub Releases.
- 📊 **Interfaz Web**: Frontend en Streamlit para filtrar salarios por institución, año, mes y buscar personas con compatibilidad para tildes y caracteres especiales (ñ).
- 🕵️ **Auditoría de Datos**: Detecta multiempleo, posible nepotismo (coincidencia de apellidos) y anomalías salariales.

## Estructura del Proyecto

```text
visor-sueldos-publicos/
├── app.py                      # Entrypoint y router de Streamlit
├── Dockerfile                  # Imagen Docker multi-stage
├── uv.lock / pyproject.toml    # Gestión de dependencias
├── scripts/                    # Scripts de ejecución manual
│   └── run_senado_extractor.py # Orquestador del scraping del Senado
├── docs/                       # Documentación técnica
│   └── api-analysis-senate.md  # Análisis de arquitectura API del Senado
├── src/
│   ├── core/                   # Lógica de negocio y base de datos
│   │   ├── api_client.py       # Cliente HTTP con retries y backoff (Tenacity)
│   │   ├── config.py           # Configuración y URLs
│   │   ├── logger.py           # Logging estructurado
│   │   └── queries.py          # Consultas SQL en DuckDB (Soporte Ñ/Tildes)
│   ├── etl/                    # Pipeline de datos
│   │   ├── ingest.py           # Transformación de CSV a Parquet
│   │   ├── senado_processor.py # Limpieza y cruce de datos del Senado (Pandas)
│   │   ├── senado_scraper.py   # Extracción paginada desde API REST
│   │   └── sync.py             # Lógica de sincronización HTTP HEAD (CPLT)
│   ├── audits/                 # Módulos de auditoría
│   │   └── audit_utils.py      # Lógica de detección de anomalías
│   └── ui/                     # Interfaz de usuario
│       └── views.py            # Componentes Streamlit y gráficos Plotly
└── .github/workflows/          # Pipelines CI/CD (Ruff, Data Sync)
```

## Desarrollo Local

Utilizamos `uv` para gestionar dependencias y asegurar builds rápidos y deterministas.

### 1. Clonar el repositorio
```bash
git clone https://github.com/felipe-veas/visor-sueldos-publicos.git
cd visor-sueldos-publicos
```

### 2. Ejecutar los Pipelines ETL
Debes procesar los datos públicos antes de levantar el frontend localmente (si no quieres usar los datos remotos por defecto).

```bash
# Sincronizar datos del Consejo para la Transparencia (Archivos CSV masivos)
uv run python src/etl/sync.py
uv run python src/etl/ingest.py

# Extraer y procesar datos del Senado de la República (API REST)
uv run python scripts/run_senado_extractor.py
```

### 3. Levantar la Aplicación Web
```bash
uv run streamlit run app.py
```

## Despliegue en Producción (Docker)

El repositorio incluye un `Dockerfile` listo para producción. Utiliza un build multi-stage, se ejecuta como usuario no-root y precompila el bytecode de Python.

```bash
docker build -t visor-sueldos .
docker run -p 8501:8501 visor-sueldos
```

## Arquitectura Serverless (GitHub Releases)

Por defecto, la aplicación **no requiere almacenamiento local** (`data/`). Para ejecutarse en plataformas Serverless, la aplicación hace fallback a URLs estáticas alojadas en GitHub Releases (`latest-data`).

1. El workflow `.github/workflows/data-sync.yml` se ejecuta periódicamente, orquesta los scrapers, empaqueta los archivos Parquet y los publica como un GitHub Release.
2. DuckDB realiza HTTP Range Requests contra las URLs del GitHub Release, obteniendo solo los bytes necesarios para la consulta SQL, logrando tiempos de respuesta de milisegundos sin descargar los archivos completos.

## Pruebas y Linter

Ejecuta la suite de validación localmente:

```bash
# Linter (Ruff)
uv run ruff check .
```

## Contribución

1. Haz un Fork del repositorio.
2. Crea una rama para tu feature (`git checkout -b feature/nueva-regla-auditoria`).
3. Haz commit de tus cambios (Asegúrate de que el código backend esté en inglés y la UI en español).
4. Verifica con `uv run ruff check .`.
5. Abre un Pull Request.

## Licencia

Proyecto de código abierto. Si utilizas estas herramientas o arquitectura para investigación académica o periodística, se agradece la atribución.
