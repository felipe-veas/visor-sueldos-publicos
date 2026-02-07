# Visor de Sueldos Públicos de Chile 🇨🇱

Este repositorio contiene herramientas en Python para descargar, unificar, auditar y visualizar los datos de **Transparencia Activa** de organismos públicos de Chile (Sueldos de Planta, Contrata y Honorarios).

El proyecto está diseñado para facilitar el análisis ciudadano y la detección de anomalías en el gasto público.

## Características

- 📥 **Descarga Automática**: Obtiene las bases de datos oficiales actualizadas desde el Consejo para la Transparencia.
- 🧹 **Normalización**: Unifica formatos dispares de sueldos y nombres de columnas.
- 📊 **Visualizador Interactivo**: Aplicación web (Streamlit) para explorar sueldos por organismo, año y mes.
- 🕵️ **Auditoría**: Módulo de inteligencia de datos que detecta:
  - **Multiempleo**: Personas con sueldos simultáneos en múltiples organismos.
  - **Nepotismo (apellidos)**: Concentración inusual de apellidos en un mismo servicio.
  - **Sueldos Atípicos**: Funcionarios que ganan significativamente más que el promedio de su estamento.

## Estructura del Proyecto

```
visor-sueldos-publicos/
├── app.py                      # Punto de entrada de la aplicación Streamlit
├── pyproject.toml              # Configuración moderna de dependencias (uv/pip)
├── requirements.txt            # Dependencias legado (para entornos sin uv)
├── src/
│   └── public_salary_monitor/  # Código fuente del paquete (Backend en Inglés)
│       ├── salary_analysis.py  # Lógica de descarga y procesamiento
│       └── audit_utils.py      # Lógica de auditoría y consultas SQL
├── tests/                      # Pruebas automatizadas
│   └── test_integration.py     # Script de prueba de integración
└── data/                       # Carpeta donde se guardan los CSV descargados
```

## Instalación y Uso

### Opción 1: Usando `uv` (Recomendado - Moderno y Rápido) 🚀

Si tienes `uv` instalado (el gestor de paquetes de Python ultra rápido):

1.  **Clonar el repositorio:**
    ```bash
    git clone https://github.com/tu-usuario/visor-sueldos-publicos.git
    cd visor-sueldos-publicos
    ```

2.  **Crear entorno virtual e instalar dependencias:**
    ```bash
    uv venv
    source .venv/bin/activate  # En Windows: .venv\Scripts\activate
    uv pip install -r pyproject.toml
    ```

3.  **Ejecutar la aplicación:**
    ```bash
    uv run streamlit run app.py
    ```

### Opción 2: Usando `pip` y `venv` (Clásico) 🐢

Si prefieres el método tradicional o no puedes usar `uv`:

1.  **Crear entorno virtual:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```

2.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Ejecutar:**
    ```bash
    streamlit run app.py
    ```

## Ejecutar Pruebas

Para verificar que todo funcione correctamente antes de ejecutar la app completa:

```bash
# Con uv
uv run python tests/test_integration.py

# Con python estándar
python tests/test_integration.py
```

## Contribución

¡Las contribuciones son bienvenidas! Si encuentras un error o quieres agregar una nueva métrica de auditoría:

1.  Haz un Fork del proyecto.
2.  Crea una rama (`git checkout -b feature/nueva-auditoria`).
3.  Haz tus cambios y commits.
4.  Abre un Pull Request.

## Licencia

Este proyecto es de código abierto. Si utilizas estos scripts para investigaciones periodísticas o académicas, se agradece la mención.
