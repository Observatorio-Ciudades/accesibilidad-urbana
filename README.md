# Accesibilidad urbana
_Hacemos referencia al acceso de diversos espacios para consolidar la investigación en perspectivas multidisciplinares de la planificación urbana y las actividades del día a día; para así comprender las necesidades colectivas y la inclusión social, mediante métodos y tecnologías para acercarnos hacia nuevas rutas de una sociedad sostenible y conectada para todos sus habitantes._

## Datos
El proyecto usa como mapa base los datos de OpenStreetMaps, descargados con [OSMnx](https://github.com/gboeing/osmnx) 
Para el cálculo de accesibilidad se usan los datos del DENUE

![ ](output/figures/Guadalajara_dist_farmacias.png)

## Métodología
Siendo la principal área de interés la aplicación de tecnologías y métodos geospaciales, se generan algoritmos que permiten calcular distancas desde cualquier punto.

Se divide la ciudad en hexágonos identificando las estructuras urbanas y condiciones de accesibilidad de cada uno.
En cada intersección al interior de la figura se toma el promedio por hexágono y se ofrece un comparativo de las diferentes áreas de la ciudad.

### Cálculos de accesibilidad
Siguiendo las rutas de camino más cortas hacia las amenidades de interés. Se consideró una velocidad promedio peatonal de 3km/hr.
En el siguiente artículo se explica a detalle [Quantifying Life Quality as Walkability on Urban Networks: The Case of Budapest](https://arxiv.org/abs/1912.00893)


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
**Contacto**

[Observatorio de Ciudades](https://observatoriodeciudades.mx/)

[Twitter](https://twitter.com/observacdstec?lang=es)

[Instagram](https://www.instagram.com/observaciudades.tec/)

