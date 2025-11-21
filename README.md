# 🎮 IMT2200 — Proyecto: Claves del éxito en videojuegos (2025-2)

Análisis de factores asociados al éxito de un videojuego utilizando datos de **IGDB**, con el objetivo de entregar recomendaciones claras para estudios independientes sobre géneros, plataformas y ventanas de lanzamiento.

---

## 📁 Estructura del repositorio

### **`data/`**  
Conjuntos de datos del proyecto.

- **`data/raw/`**  
  Datos crudos descargados desde la API de IGDB:
  - `igdb_empresas.csv`
  - `igdb_generos.csv`
  - `igdb_plataformas.csv`
  - `igdb_involved_companies.csv`
  - `juegos_raw.csv`

- **`data/processed/`**  
  Datos limpios listos para análisis:
  - `igdb_empresas_limpio.csv`
  - `igdb_generos_limpio.csv`
  - `igdb_plataformas_limpio.csv`
  - `igdb_involved_companies_limpio.csv`
  - `juegos_igdb_limpio.csv`

---

### **`notebooks/`**  
Jupyter Notebooks que documentan el flujo del proyecto.

#### **`notebooks/api/`**
Pruebas y prototipos con la API de IGDB.
- `igdb_api_request.ipynb`
- `test.ipynb`

#### **Limpieza**
- `00_limpieza_raw_g.ipynb`  
  Limpieza del dataset de géneros.

- `01_limpieza_auxiliares_igdb.ipynb`  
  Limpieza de plataformas, empresas e involved_companies.

#### **EDA**
- `02_eda_igdb.ipynb`  
  Exploración de datos: análisis temporal, distribución por géneros, plataformas, estudios y definición de la variable `exitoso`.

#### **Modelado**
- `03_modelo.ipynb`  
  Construcción del dataset final, codificación de variables, entrenamiento de modelos de clasificación y análisis de resultados.

#### **Otros**
- `Conclusiones.ipynb`  
  Integración de resultados del EDA y del modelado.
- `notebooks/antiguo/`  
  Notebooks previos/experimentales mantenidos como referencia.

---

### **`codigos/`**
Scripts auxiliares del proyecto.

- `descarga.py`  
  Funciones para descargar datos paginados desde la API y guardarlos en `data/raw/`.

---

### **`docs/`**
Documentos del proyecto.
- `Propuesta Inicial.pdf`

---

### **`img/`**
Imágenes utilizadas en informes, presentaciones o notebooks.

---

### **`README.md`**
Descripción del proyecto, flujo de trabajo y estructura del repositorio.

---

## 📊 Flujo del proyecto

### **1. Descarga de datos**
- Uso de la API de IGDB mediante notebooks en `notebooks/api/` y funciones en `codigos/descarga.py`.
- Almacenamiento de entidades crudas en `data/raw/`.

### **2. Limpieza y preprocesamiento**
- Limpieza de géneros, plataformas, empresas, tablas relacionales y dataset principal de juegos.
- Conversión de fechas, estandarización de columnas, parseo de IDs, manejo de duplicados.
- Exportación a `data/processed/`.

### **3. Análisis exploratorio (EDA)**
- Clasificación de juegos (AAA, No AAA, Sin publisher).
- Definición de la variable `exitoso`.
- Análisis por género, plataforma, año y mes de lanzamiento.
- Identificación de tendencias relevantes para estudios independientes.

### **4. Modelado predictivo**
- Construcción del dataset de modelado.
- Entrenamiento de modelos de clasificación.
- Evaluación e interpretación de resultados.

### **5. Resumen final**
- El análisis integrado del proyecto (EDA + modelado + conclusiones estratégicas) se encuentra en  
  **`Conclusiones.ipynb`** ubicado en la carpeta **raíz del repositorio**.

---

## 🧠 Principales hallazgos

- Solo una fracción del catálogo posee ratings completos; el análisis de éxito se restringe a estos títulos.
- Existen géneros con volumen alto pero tasas moderadas de éxito, y géneros más pequeños con oportunidades para indies.
- Los títulos AAA concentran más reseñas y mayor tasa de éxito, aunque los estudios no AAA logran buenos resultados bajo ciertas combinaciones de género y plataforma.
- PC concentra la mayor parte de los lanzamientos indies; consolas muestran proporciones de éxito relativamente más altas.
- No existe una ventana única de lanzamiento que asegure éxito.
- Factores más relevantes en el éxito:
  - tipo de estudio,
  - género(s),
  - categoría de plataforma,
  - año y mes de lanzamiento.

---

Pontificia Universidad Católica de Chile — IMT2200 (2025-2)
