# Modelo Predictivo de Potencial de Inversión Inmobiliaria en la CDMX

> Un proyecto de ciencia de datos end-to-end para identificar las zonas con mayor potencial de crecimiento y rentabilidad en el mercado inmobiliario de la Ciudad de México.

Este repositorio contiene el pipeline de ingeniería de datos para un modelo de aprendizaje supervisado cuyo objetivo es clasificar zonas geográficas (Unidades Territoriales) de la CDMX según su potencial de inversión. El proyecto integra un conjunto de datos multidimensional que abarca variables sociodemográficas, urbanas, de actividad económica y de seguridad para construir un set de features robusto y listo para el modelado.

---

## Características Principales

* **Pipeline de ETL Modular:** El procesamiento de datos está orquestado por un script principal (`main.py`) que llama a funciones modulares, siguiendo las mejores prácticas de software para reproducibilidad y mantenimiento.
* **Integración de Múltiples Fuentes de Datos:** Unifica y procesa datos de diversas fuentes públicas:
    * **Demográficos:** INEGI (Censo 2020).
    * **Económicos:** INEGI (DENUE Histórico).
    * **Seguridad:** Datos Abiertos CDMX (Carpetas de Investigación FGJ).
    * **Geoespaciales:** Marco Geoestadístico del INEGI y Mapa de Colonias del IECM.
* **Ingeniería de Características Geoespaciales:** Realiza operaciones complejas como la **interpolación areal** para transferir datos de una granularidad (AGEB) a otra (Unidad Territorial) de forma precisa.
* **Herramientas de Línea de Comandos:** Incluye scripts de utilidad para validar la calidad del dataset final (`validate_final_dataset.py`) y para realizar consultas rápidas (`query_stats.py`), separando la producción de datos del análisis.
* **Visualización de Hallazgos:** Genera un mapa interactivo (`hallazgos_clave_mapa.html`) que permite explorar las relaciones entre las variables clave.

---

## Estructura del Repositorio

```
potencial-inmobiliario-cdmx/
│
├── config/
│   └── params.yaml             # Archivo central de configuración (rutas, parámetros, etc.)
│
├── data/
│   └── 01_raw/                 # Directorio para los datos crudos (descargados por el usuario)
│
├── notebooks/                  # Notebooks para exploración y análisis
│
├── src/
│   ├── data/make_dataset.py    # Funciones para cargar y limpiar datos crudos
│   ├── diagnostics/inspector.py# Módulo de diagnóstico para depuración
│   ├── features/build_features.py # Funciones para crear las variables (features)
│   └── visualization/visualize.py # Funciones para generar mapas y gráficos
│
├── main.py                     # Orquestador principal del pipeline de ETL
├── query_stats.py              # Script para realizar consultas rápidas al dataset final
├── validate_final_dataset.py   # Script para generar un reporte de calidad del dataset final
├── create_story_map.py         # Script para generar el mapa interactivo de hallazgos
├── requirements.txt            # Lista de dependencias de Python
└── README.md                   # Este archivo
```

---

## Instalación y Configuración

Para configurar el entorno y ejecutar el proyecto localmente, siga estos pasos:

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/su-usuario/potencial-inmobiliario-cdmx.git](https://github.com/su-usuario/potencial-inmobiliario-cdmx.git)
    cd potencial-inmobiliario-cdmx
    ```

2.  **Crear un entorno virtual (recomendado):**
    ```bash
    python -m venv env
    source env/bin/activate  # En Windows: env\Scripts\activate
    ```

3.  **Instalar las dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

### Fuentes de Datos y Configuración

**Este repositorio no incluye los archivos de datos crudos para mantener un tamaño de repositorio manejable.** Para ejecutar el pipeline, primero debe descargar los siguientes datasets y colocarlos en la estructura de carpetas especificada dentro de `data/01_raw/`.

1.  **Censo de Población y Vivienda 2020 (INEGI):**
    * **Descripción:** Resultados por AGEB urbana de la CDMX.
    * **Fuente:** [INEGI - Censo 2020](https://www.inegi.org.mx/programas/ccpv/2020/default.html#Datos_abiertos)
    * **Archivo esperado:** `conjunto_de_datos_ageb_urbana_09_cpv2020.csv`
    * **Ubicación final:** `data/01_raw/Censo/conjunto_de_datos_ageb_urbana_09_cpv2020.csv`

2.  **Directorio Estadístico Nacional de Unidades Económicas (DENUE Histórico):**
    * **Descripción:** Múltiples archivos ZIP con los snapshots históricos del DENUE para la CDMX.
    * **Fuente:** [INEGI - DENUE](https://www.inegi.org.mx/servicios/denue/Default.aspx)
    * **Archivos esperados:** `denue_*.zip`
    * **Ubicación final:** `data/01_raw/DENUE/` (todos los archivos ZIP deben ir aquí).

3.  **Carpetas de Investigación FGJ (Datos Abiertos CDMX):**
    * **Descripción:** Dataset de carpetas de investigación de la Fiscalía General de Justicia.
    * **Fuente:** [Datos Abiertos CDMX](https://datos.cdmx.gob.mx/dataset/carpetas-de-investigacion-fgj-de-la-ciudad-de-mexico)
    * **Archivo esperado:** `carpetasFGJ_acumulado_AAAA_MM.csv` (o el nombre que tenga al descargarlo).
    * **Ubicación final:** `data/01_raw/Seguridad/carpetasFGJ_acumulado_AAAA_MM.csv`

4.  **Geometrías (Shapefiles):**
    * **AGEBs Urbanas:** Del Marco Geoestadístico del INEGI.
        * **Ubicación final:** `data/01_raw/poligono_ageb_urbanas_cdmx/`
    * **Colonias CDMX:** Del Instituto Electoral de la Ciudad de México (IECM).
        * **Ubicación final:** `data/01_raw/colonias_cdmx/`

Una vez que haya colocado todos los archivos en sus respectivas carpetas, asegúrese de que las rutas en `config/params.yaml` coincidan con los nombres de sus archivos.

---

## Uso

* **Ejecutar el Pipeline Completo:**
    Para procesar todos los datos desde cero y generar el dataset final en `data/03_primary/`.
    ```bash
    python main.py
    ```

* **Validar el Dataset Final:**
    Genera un reporte de calidad completo en la consola.
    ```bash
    python validate_final_dataset.py
    ```

* **Realizar Consultas Rápidas:**
    ```bash
    # Resumen de población
    python query_stats.py population

    # Resumen de datos económicos
    python query_stats.py denue
    ```

* **Generar el Mapa de Hallazgos:**
    Crea el archivo `hallazgos_clave_mapa.html`.
    ```bash
    python create_story_map.py
    ```
---

## Próximos Pasos

Con la fase de ingeniería de datos concluida, el proyecto se centrará en:

1.  **Construcción de la Variable Objetivo (Y):** Utilizar datos históricos de precios para calcular la **plusvalía** y derivar las clases de potencial de inversión (Bajo, Medio, Alto).
2.  **Modelado Predictivo:** Entrenar y evaluar modelos de clasificación (Random Forest, XGBoost).
3.  **Interpretabilidad del Modelo:** Aplicar técnicas como SHAP para entender los factores más influyentes en las predicciones.