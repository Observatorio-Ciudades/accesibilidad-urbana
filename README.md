# Accesibilidad urbana

_Medir la accesibilidad urbana a diferentes amenidades relevantes durante el COVID-19._

## Datos
El proyecto usa como mapa base los datos de OpenStreetMaps, descargados con [OSMnx](https://github.com/gboeing/osmnx) 
Para el cálculo de accesibilidad se usan los datos del DENUE

!()[output/figures/GDL_distfarmacias.png]


### Estructura de proyecto
------------

La estructura de los folders de este proyecto es:

```
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`.
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
├── output            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures       <- Generated graphics and figures to be used in reporting
|   └── text          <- Reports
│
└── src                <- Source code for use in this project.
    ├── __init__.py    <- Makes src a Python module
    │
    ├── data.py        <- Scripts to download or generate data
    │
    ├── analysis.py    <- Scripts to analyse the data
    │
    └── visualization.py  <- Scripts to create exploratory and results oriented visualizations

```
