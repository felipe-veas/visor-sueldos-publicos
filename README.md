# Visor de Sueldos Públicos de Chile 🇨🇱

Este repositorio contiene herramientas Python para descargar, unificar, auditar y visualizar datos de Transparencia Activa de instituciones públicas chilenas.

El sistema utiliza DuckDB y Parquet para consultar más de 28GB de datos en milisegundos. Está diseñado para ejecutarse sin infraestructura backend dedicada.

## Características Principales

- 📥 **Smart Sync**: Consulta los servidores de transparencia y descarga archivos solo cuando los headers remotos indican cambios, reduciendo el uso de ancho de banda.
- ⚡️ **Motor de Consultas**: Convierte 28GB de CSVs crudos a archivos `.parquet` con compresión ZSTD. Utiliza DuckDB para consultas en memoria.
- ☁️ **Despliegue Stateless**: Arquitectura de 3 capas (MVC) diseñada para entornos contenerizados. Utiliza `httpfs` para lectura remota de archivos.
- 📊 **Interfaz Web**: Frontend en Streamlit para filtrar salarios por institución, año y mes.
- 🕵️ **Auditoría de Datos**: Detecta multiempleo, posible nepotismo (coincidencia de apellidos) y anomalías salariales.

## Estructura del Proyecto

```text
visor-sueldos-publicos/
├── app.py                      # Entrypoint y router de Streamlit
├── Dockerfile                  # Imagen Docker multi-stage
├── uv.lock / pyproject.toml    # Gestión de dependencias
├── src/
│   ├── core/                   # Lógica de negocio y base de datos
│   │   ├── config.py           # Configuración y URLs
│   │   └── queries.py          # Consultas SQL en DuckDB
│   ├── etl/                    # Pipeline de datos
│   │   ├── sync.py             # Lógica de sincronización HTTP HEAD
│   │   └── ingest.py           # Transformación de CSV a Parquet
│   └── ui/                     # Interfaz de usuario
│       └── views.py            # Componentes Streamlit y gráficos Plotly
└── .github/workflows/          # Pipelines CI/CD (Ruff, Pytest, Releases)
```

## Desarrollo Local

Utilizamos `uv` para gestionar dependencias y asegurar builds rápidos y deterministas.

### 1. Clonar el repositorio
```bash
git clone https://github.com/tu-usuario/visor-sueldos-publicos.git
cd visor-sueldos-publicos
```

### 2. Ejecutar el Pipeline ETL
Debes procesar los datos públicos antes de levantar el frontend. El script de sincronización descarga los CSVs oficiales y los comprime a Parquet.

```bash
# Instalar dependencias y ejecutar la sincronización
uv run src/etl/sync.py
```
*(Nota: La descarga inicial de 28GB tomará tiempo dependiendo de tu conexión. Las ejecuciones posteriores toman milisegundos si los datos remotos no han cambiado).*

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

Por defecto, la aplicación lee del directorio local `data/`. Para ejecutarla sin volúmenes de almacenamiento dedicados, la aplicación hace fallback a URLs estáticas alojadas en GitHub Releases.

1. El workflow `.github/workflows/data-sync.yml` se ejecuta semanalmente, empaqueta los archivos Parquet y los publica como un GitHub Release.
2. Si la aplicación se ejecuta sin un directorio `data/` local, DuckDB realiza HTTP Range Requests contra las URLs del GitHub Release, obteniendo solo los bytes necesarios para la consulta.

## Pruebas y Linter

Ejecuta la suite de validación localmente:

```bash
# Linter
uv run ruff check .

# Pruebas unitarias
uv run pytest tests/
```

## Contribución

1. Haz un Fork del repositorio.
2. Crea una rama para tu feature (`git checkout -b feature/nueva-regla-auditoria`).
3. Haz commit de tus cambios y verifícalos con `ruff`.
4. Abre un Pull Request.

## Licencia

Proyecto de código abierto. Si utilizas estas herramientas o arquitectura para investigación académica o periodística, se agradece la atribución.
