# Observatorio de Ciudades 
_Una iniciativa de la Escuela de Arquitectura, Arte y Diseño del Tecnológico de Monterrey; un espacio que ofrece información actual y útil que permite el análisis, medición y evaluación de las ciudades mexicanas, para incidir en la planeación y la toma de decisiones que contribuirán a mejorar la calidad de vida urbana de las personas._    

_Analiza y compara distintas ciudades, integrando bases de datos existentes y generando información propia, para entender las condiciones en el territorio que determinan la calidad de vida urbana. El observatorio de ciudades estudia los entornos a escala barrial/vecinal para comprender dinámicas y factores que condicionan la calidad de vida de las personas._

## Contenido 
1. [Objetivos](#objetivos)
2. [Bases de datos](#bases-de-datos)
3. [Servicios](#servicios)
4. [Contacto](#contacto)

## Objetivos:

**Generar y transferir conocimiento**

>El Observatorio de Ciudades busca analizar información de bases de datos oficiales, bigdata y análisis propio a través de tecnologías de vanguardia para evaluar los niveles de inclusión y planeación de ciertas ciudades.

**Impactar en la academia y la investigación**

>A través de las conclusiones derivadas del proyecto, buscamos promover la creación de más iniciativas que generen impacto en los usuarios para generar un bienestar que puede ser significativo. La academia y la investigación son dos pilares fundamentales de la innovación y el conocimiento, tanto en la divulgación de temas que representan un papel decisivo en el espacio educativo como en plataformas para fomentar acción dentro de nuestras ciudades.


## Bases de datos 
Se emplean las siguientes herramientas de información geográfica para evaluar indicadores que permitan abordar las problemáticas más relevantes del proyecto. 

![ ](output/figures/Guadalajara_dist_farmacias.png)

[**H3**](https://h3geo.org/docs/)
***
Es un sistema de índices geoespaciales que dividen el territorio por medio de celdas hexagonales. Cada polígono incluye coordenadas que permiten encontrar límites dentro de cada arista. Calcula de manera coherente los datos demográficos de acuerdo con las coordenadas y dependiendo su área. Los hexágonos representan una forma estandarizada de comparar diferentes partes de la ciudad en parcelas del mismo tamaño.      
[**DENUE**](https://www.inegi.org.mx/app/mapa/denue/default.aspx)
***
El Directorio Estadístico Nacional de Unidades Económicas (DENUE) ofrece datos cuantitativos, de ubicación y actividad económica de unidades en el territorio nacional.      
[**CENSO DEL INEGI**](https://www.inegi.org.mx/programas/ccpv/2020/default.html#Resultados_generales)
***
Se utilizan los datos más actualizados del  Censo de Población y Vivienda 2020 para analizar las características demográficas, socioeconómicas y culturales del país.      
[**MARCO GEOESTADÍSTICO:**](https://www.inegi.org.mx/temas/mg/#Mapa)  
***
El mapa del marco geoestadístico muestra de manera precisa límites estatales y municipales que hacen referencia a establecimientos económicos, viviendas y unidades de producción del país, permitiendo su consulta detallada de cualquier parte del territorio nacional.      
[**PLANETARY COMPUTER**](https://planetarycomputer.microsoft.com/:)
***
Una Plataforma diseñada para monitorear procesos y datos de manera global en temas ambientales con APIs intuitivos. Dichos recursos muestran imágenes satelitales que permiten utilizar los resultados para un proceso de toma de decisiones más sostenible.      
[**OSMnx**](https://osmnx.readthedocs.io/en/latest/)
***
Fuentes como OpenStreetMap son utilizadas como complemento, la información (VGI) nos da pie a datos relevantes. OSMnx es un paquete para interactuar dentro de Python con información geográfica construida con NetworkX, geopandas y matplotlib para facilitar el análisis y cálculo estadístico espacial.  
## Servicios

_El Observatorio de Ciudades busca proporcionar servicios de utilidad urbana aportando la experiencia y la evaluación de diferentes herramientas para informar acerca de temas de geolocalización enfocada al urbanismo, mismas que pueden ayudar a diversos sectores a crear ciudades inclusivas._

**Análisis de imágenes satelitales**
***
Las imágenes son una valiosa fuente de información, pero aprovechar los datos que te brindan puede ser un reto. Usando nuestra experiencia y análisis puedes obtener información detallada, y reportes que aprovechan los datos extraídos de distintos satélites a través de Planetary Computer y  el estudio de índices como NDVI, NDMI o temperatura que permiten la identificación de características únicas, para obtener mayores características de territorios dentro de tu área de interés.

![](https://raw.githubusercontent.com/avdesni/Imagenes_Observatorio/main/NDVI_AGS_IMGN_SAT.png)


**Análisis sociodemográfico**
***
La composición de la población y sus procesos demográficos son un factor importante para evaluar el desarrollo de la región, su uso como indicador es un instrumento fundamental para contribuir en la toma de decisiones, elaboración de políticas e investigación en el desarrollo urbano. El equipo valida y analiza información sociodemográfica local, regional y nacional de los censos del INEGI mejorando las oportunidades de mercado y cuantificando la demanda de prioridades que utilicen las variables en un enfoque espacial para obtener información territorial de circunstancias, contexto y características de cualquier espacio. 

![](https://raw.githubusercontent.com/avdesni/Imagenes_Observatorio/main/QRO__ANÁLISIS_SOCIODEMOGRÁFICO.png)

**Análisis de proximidad**
***
La relación que subyace entre recorridos puede revelar patrones y tendencias en la distribución espacial de un territorio. El método se basa en la vinculación de sistemas de información geográfica evaluados a través de la generación de algoritmos que permiten calcular tanto las distancias como sus relaciones desde y hacia cualquier punto.
La morfología urbana es considerada a partir de nodos y aristas viales que se obtienen utilizando OSMnx. Se divide la ciudad en hexágonos identificando las estructuras urbanas ofreciendo un comparativo de distancias hacia el punto de interés.


![](https://raw.githubusercontent.com/avdesni/Imagenes_Observatorio/main/Prox_index.png)

**Visores**
***
Para que las ciudades sean inclusivas, seguras, resilientes y sostenibles es necesario que los datos se conviertan en conocimientos aplicables que transformen la política y el entorno.

El visor urbano presenta una exposición interactiva donde es posible analizar información sobre diferentes aspectos de una ciudad mediante la comparación y el contraste de territorio, nos acerca a historias y contextos por medio su visualización directa, dinámica e informativa para el monitoreo y mejora de la planificación urbana. Es un acercamiento hacia la accesibilidad hecha para el peatón y basada en el concepto de movilidad conocido como [_Ciudades de 15 minutos_](https://www.mdpi.com/2624-6511/4/1/6) en donde los servicios y amenidades deberían existir a una distancia menor a 15 minutos, para lograr ese objetivo es necesario tener información precisa sobre la ubicación de servicios, transporte y negocios.   

Los visores dan pie a la recopilación de esta información entendiendo a profundidad la distribución geográfica de los servicios y equipamientos; promoviendo un enfoque urbanístico de proximidad que puede aumentar los niveles de sostenibilidad de las zonas al tiempo que incrementa la calidad de vida urbana.


![](https://raw.githubusercontent.com/avdesni/Imagenes_Observatorio/main/Visores.png)


## Contacto

[Página web](https://observatoriodeciudades.mx/)

[Twitter](https://twitter.com/observacdstec?lang=es)

[Instagram](https://www.instagram.com/observaciudades.tec/)

