# Sistema de Análisis Global de Países

## Problema que Resuelve

El Sistema de Análisis Global de Países resuelve el desafío de recopilar, procesar y visualizar datos económicos de múltiples países para facilitar la toma de decisiones en inversiones y análisis económico. Específicamente:

* **Datos Dispersos**: Centraliza indicadores económicos clave (inflación, desempleo, PIB per cápita, tasas de interés) que están dispersos en diferentes fuentes.
* **Complejidad de Análisis**: Simplifica la comparación entre países mediante la creación de un Índice de Prosperidad Económica que consolida múltiples variables.
* **Actualización Manual**: Automatiza la recolección y procesamiento de datos que tradicionalmente requeriría actualizaciones manuales frecuentes.
* **Visualización Integrada**: Prepara los datos para ser visualizados en herramientas como Power BI, eliminando la necesidad de procesamientos adicionales.

## Tecnologías Utilizadas

### Backend y Procesamiento de Datos
* **Python 3.8+**: Lenguaje principal de desarrollo
* **Pandas/NumPy**: Manipulación y análisis de datos
* **SQLAlchemy**: ORM para interacción con la base de datos
* **Requests**: Consumo de APIs (World Bank)

### Almacenamiento
* **PostgreSQL**: Base de datos relacional para almacenamiento estructurado
* **Estructura ETL**: Extracción, Transformación y Carga de datos

### APIs y Fuentes de Datos
* **World Bank API**: Fuente principal de indicadores económicos
* **Freedom Index** (implementación futura): Datos sobre libertad política
* **News API** (implementación futura): Análisis de sentimiento de noticias

### Visualización (Planificada)
* **Power BI**: Dashboards interactivos para análisis visual

## Arquitectura y Metodología

El sistema sigue una arquitectura ETL (Extracción, Transformación, Carga) con componentes modulares:

### 1. Extracción (Extraction)
* **Extractores Especializados**: Módulos dedicados para cada fuente de datos
* **World Bank Extractor**: Obtiene datos económicos de países mediante la API del Banco Mundial
* **Mecanismos de Reintentos**: Manejo de errores y reintentos para garantizar la obtención de datos

### 2. Transformación (Transformation)
* **Normalización de Datos**: Limpieza y estructuración de datos crudos
* **Cálculo de Índices**: Generación del Índice de Prosperidad Económica mediante algoritmos ponderados
* **Transformadores Específicos**: Módulos para cada tipo de transformación y cálculo

### 3. Carga (Load)
* **Esquema Relacional**: Base de datos PostgreSQL con tablas normalizadas
* **Carga Incremental**: Actualización de registros existentes o inserción de nuevos datos
* **Registro de Operaciones**: Logging detallado de todas las operaciones ETL

### 4. Orquestación
* **Script Principal**: Coordina todo el proceso ETL
* **Control de Flujo**: Manejo de errores y dependencias entre pasos
* **Configuración Flexible**: Parametrización mediante variables de entorno

## Resultados y Métricas

### Métricas Cuantificables
* **Datos Procesados**: Capacidad para procesar datos de 217 países
* **Indicadores por País**: 4 indicadores económicos clave por país (inflación, desempleo, PIB per cápita, tasas de interés)
* **Cobertura Temporal**: Datos históricos desde 2015 hasta la actualidad
* **Velocidad de Procesamiento**: Pipeline completo ejecutado en aproximadamente 15 segundos
* **Tasa de Éxito**: 100% de los países con datos disponibles procesados correctamente
* **Índice Calculado**: Índice de Prosperidad Económica en escala 0-100 para cada país/año

### Beneficios Funcionales
* **Normalización**: Datos de diferentes fuentes estandarizados en un formato consistente
* **Automatización**: Eliminación de procesamiento manual de datos
* **Análisis Comparativo**: Capacidad para comparar fácilmente indicadores entre diferentes países
* **Centralización**: Una única fuente de verdad para análisis económico

## Próximos Pasos

### Corto Plazo
1. **Integración de Freedom Index**: Implementar web scraping para obtener datos del Índice de Libertad
2. **Cálculo del Índice de Estabilidad Política**: Utilizando datos del Freedom Index y otros indicadores
3. **Optimización de Rendimiento**: Mejoras en consultas y procesos de carga para mayor eficiencia

### Mediano Plazo
1. **Integración con News API**: Implementar extracción de noticias por país
2. **Análisis de Sentimiento de Noticias**: Aplicar NLP para determinar sentimiento positivo/negativo
3. **Cálculo del Índice de Oportunidad de Inversión**: Combinar indicadores económicos, estabilidad política y sentimiento

### Largo Plazo
1. **Dashboard en Power BI**: Desarrollo de visualizaciones interactivas para análisis
2. **Orquestación con Airflow**: Implementar DAGs para programación y monitoreo del pipeline
3. **API de Consulta**: Crear API REST para acceder a los datos y índices calculados
4. **Predicciones y Tendencias**: Implementar modelos predictivos para anticipar tendencias económicas

## Instalación y Uso

### Requisitos Previos
- Python 3.8+
- PostgreSQL 12+
- Acceso a Internet (para APIs)

### Pasos de Instalación
1. Clonar el repositorio
2. Crear un entorno virtual: `python -m venv venv`
3. Activar el entorno virtual: `source venv/bin/activate` (Linux/Mac) o `venv\Scripts\activate` (Windows)
4. Instalar dependencias: `pip install -r requirements.txt`
5. Configurar variables de entorno en archivo `.env` basado en `.env.example`

### Uso Básico
```bash
# Inicializar la base de datos
python src/main_etl.py --init-db

# Ejecutar el proceso ETL completo
python src/main_etl.py --countries US,GB,DE,JP,BR,IN,ZA

# Solo extraer datos (sin calcular índices)
python src/main_etl.py --extract-only --countries US,GB,DE

# Solo calcular índices (sin extraer nuevos datos)
python src/main_etl.py --calculate-only --countries US,GB,DE
```

## Estructura del Repositorio

```
global-country-analysis/
│
├── data/                       # Datos intermedios y procesados
├── logs/                       # Logs de ejecución
├── src/                        # Código fuente
│   ├── extractors/             # Módulos de extracción
│   ├── transformers/           # Módulos de transformación
│   ├── loaders/                # Módulos de carga
│   └── database/               # Esquema y utilidades de BD
├── .env.example                # Ejemplo de variables de entorno
└── requirements.txt            # Dependencias del proyecto
```
