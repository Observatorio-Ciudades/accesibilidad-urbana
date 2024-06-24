#Código GeoVisor App

### Librerías ###
import geopandas as gpd 
import pandas as pd
import folium #Librería del mapa interactivo
from folium.plugins import FeatureGroupSubGroup #Librería que se utiliza para generar las capas que se prenden y apagan
import streamlit as st
import streamlit_folium as stf #Librería de folium en streamlit
from folium import GeoJson #Librería para generar el checkmark de las capas
import matplotlib.pyplot as plt
import plotly.graph_objects as go #Librería para generar el spyder plot
import os 
import sys
#module_path = os.path.abspath(os.path.join('../../'))
#if module_path not in sys.path:
 #   sys.path.append(module_path)
  #  import aup

### Funciones ###
#Función para leer los archivos gpd y cambiarles el formato de coordenada al que usamos siempre
def read_file(filepath):
    return gpd.read_file(filepath).to_crs('EPSG:4326') 

#Función para agregar un geodataframe como capa al mapa interactivo
def add_gdf_to_map(gdf, name, color): #Nombre del geodataframe que se va a mostrar, nombre que se va a mostrar en el mapa, color que se va a presentar
    g = FeatureGroupSubGroup(m, name) #Se genera una capa con el geodataframe y el nombre
    m.add_child(g) #Se agrega la capa al mapa
    
    fields = [field for field in gdf.columns if field != 'geometry'] #
    
    GeoJson( #Formato del checkmark que se agrega en la capa 'g'
        gdf,
        zoom_on_click=True,
        style_function=lambda feature: {
            'fillColor': color,
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6,
        },
        highlight_function=lambda x: {'weight': 3, 'color': 'black'},
        tooltip=folium.GeoJsonTooltip(fields=fields, labels=True, sticky=True) #Este es el checkmark

    ).add_to(g)

def add_gdf_marker(gdf, name, color, icon_name, icon_color): #Este es igual que el anterior pero se generó para los que son íconos en este mapa, ósea los bomberos, salud y educación.
    g = FeatureGroupSubGroup(m, name)
    m.add_child(g)
    
    fields = [field for field in gdf.columns if field != 'geometry']
    
    GeoJson(
        gdf,
        zoom_on_click=True,
        style_function=lambda feature: {
            'fillColor': color,
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6,
        },
        highlight_function=lambda x: {'weight': 3, 'color': 'black'},
        tooltip=folium.GeoJsonTooltip(fields=fields, labels=True, sticky=True)
    ).add_to(g) #Hasta aquí todo es igual que la función pasada
    for _, row in gdf.iterrows(): #Se saca la coordenada de los puntos de interés
        lat = row.geometry.y #Latitud
        lon = row.geometry.x #Longitud
            
        folium.Marker(
            location=[lat, lon],
            icon=folium.Icon(color=icon_color, prefix='fa', icon=icon_name) #Necesita tener el prefijo fa porque así está definido el formato para los íconos
        ).add_to(g) #Teniendo la latitud y longitud se pone el ícono con el folium marker

### Variables de entrada ###
dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/' #El directorio de donde tengo guardado la carpeta de toda la data
buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp') #Data del buffer de la Nueva Alameda
poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp') #Data del polígono de la metropolitana de Santiago
bomberos = read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp') #Data de los bomberos Chile Nación
columns = ['13102','13112','13115','13125','13105','13101', '13108','13111','13114'] #Códigos postales de la zona metropolitana Santiago
bomberos = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns)] #Filtrar solo los bomberos de la metropolitana Santiago
salud = read_file(dir + 'Establecimientos salud/establec_salud_14_mayo_2021.shp') #Data Salud Metro Santiago
salud = salud[salud['nom_provin'] == 'Santiago'].sample(n=100, random_state=43) #Filtrar solo 100 puntos de forma aleatoria para la visualización más rápida del mapa
educ = read_file(dir + 'Establecimientos educacionales/establecimientos_educacionales_2021.shp') #Data educ metro Santiago
educ = educ[educ['nom_provin'] == 'Santiago'].sample(n=100, random_state=43) #Filtrar solo 100 puntos de forma aleatoria para la visualización más rápida del mapa
#hex_schema = "projects_research"
#hex_table = "santiago_hexproximity_hqsl_4_5_kmh"
#n = '9'
#query = f'SELECT * FROM {hex_schema}.{hex_table} WHERE \"res\" = {n}'
#SPYDER = aup.gdf_from_query(query, geometry_col='geometry') ### No pude agregar esto porque no tengo docker por el trojano, entonces descargue la base de datos ###
spyderplot = pd.read_csv('OneDrive/Documentos/GitHub/accesibilidad-urbana/notebooks/Alameda_Santiago/funcionessociales.csv') #Data de las 6 Funciones sociales y columna geometría
santiago = spyderplot.drop(spyderplot.columns[0], axis=1) #Data sin columna Geometry
columns_santiago = santiago.columns #Columnas y valores

# Configuración inicial de la página de Streamlit
st.set_page_config(
    page_title='Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile',
    page_icon=":wine_glass:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título y descripción de la página
st.title('Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile')
st.write("Geovisor con la información más relevante de la zona")

#Sidebar en donde se van a poner las instrucciones
with st.sidebar:
    st.write("Instrucciones de uso del geovisor") #Para escribir cosas. Si quieres poner título es con st.title y st.subtitle

select = st.selectbox( #Dropdown Menu con cada uno de las 6 funciones sociales
    "Seleccione la función social",
    ["Supplies", "Caring", "Living", 
    "Enjoy", "Working", "Sociability"]
)
#Función Social 1
if select == "Supplies":
    tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"]) #Se generan las tabs para cada uno de los usuarios
    with tab1: #Código para el primer tab
        st.write("Aquí va a ir texto")
        with st.container(): #Se genera un contenedor en donde se va a mostrar el mapa interactivo con las legends de las cosas
            col1, col2 = st.columns([0.8,0.2]) #80% del contenedor será para el col1(Mapa Interactivo) y 20 para las legends
            with col1: 
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45) #Se genera el mapa de Santiago, incializado en el centro de la alameda
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue") #Se agrega la capa del buffer
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green") #Se agrega la capa de la metro Santiago
                add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red') #Se agrega la capa de bomberos filtrados
                folium.LayerControl(collapsed=False).add_to(m) #Se pone un control en las capas para que no se sobreescribam
                st.title("Mapa interactivo") #Título del mapa
                st_data = stf.st_folium(m, width=1000, height=700) #Se usa la función de folium de streamlit para mostrar el mapa
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f""" 
                    <span style="display: inline-flex; align-items: center;"> 
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True) #Se genera el primer legend con css
                    #Solo se cambiarían los background color y el nombre, en este caso esta Área Metropolitana de Santiago
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True) #Se genera el segundo legend con css
                    #Igual que el pasado
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Compañias de Bomberos</span>
                    </span>
                    """, unsafe_allow_html=True) #Se genera el tercer legend con css
                    #Aquí se agregó un url que es de donde se sacan los íconos mostrados, y se le agrego una línea para el ícono
        st.write("")
        with st.container(): #Depues de generar el container del mapa, justo abajo se genera otro container pero ahora para las gráficas.
            col1, col2, col3 = st.columns([0.33,0.33,0.33]) #Como tenemos gráficas para Santiago, Alameda y Barrios, se divide en 3.
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                column_sums = santiago.sum() #Se suman los valores de cada una de las columnas.
                labels = columns_santiago #Se agarran los nombres de las columnas para labels de la gráfica
                sums = column_sums.values #Se obtienen los valores numéricos de la suma

                fig = go.Figure() #Se genera el plot con la función 'go' de Plotly

                fig.add_trace(go.Scatterpolar(
                    r=sums,
                    theta=labels,
                    fill='toself'
                )) #Se genera el spyderplot con popups. Se muestra la suma en la variable 'r' y los títulos en la variable 'theta'. Se pueden cambiar estas variables y se vería el cambio en la gráfica.

                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, max(sums)]
                        )),
                    showlegend=False
                ) #Se genera el layout del grid de la gráfica
                st.plotly_chart(fig) #Se muestra en la app
                st.write("Datos")
            with col2: # es lo mismo que el pasado
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['13101','13102']
                bomberos_alameda = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                frecuencia = bomberos_alameda['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3: #Y esto igual pero se genera un dropdown menu para que cambie dependiendo del barrio que se busca
                barrio = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "bomberos",
                    label_visibility="collapsed",
                )
                if barrio == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['13125']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['13105']
                    bomberos_barrio2 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio2['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['13114']
                    bomberos_barrio3 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio3['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['13111']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab2: #Es exactamente lo mismo que el tab 1, solo que aquí cambia el mapa
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(educ, "Establecimiento educacionales", "green", 'graduation-cap', 'green')
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-graduation-cap" style="color: green; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimiento educacionales</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = educ['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                educ_alameda = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = educ_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_educ = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "educ",
                    label_visibility="collapsed",
                )
                if barrio_educ == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    educ_barrio1 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    educ_barrio2 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    educ_barrio3 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    educ_barrio4 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab3: #Igual que el anterior, solo cambia el mapa.
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')    
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-heartbeat" style="color: orange; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimientos de Salud</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = salud['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                salud_alameda = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = salud_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_salud = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "salud",
                    label_visibility="collapsed",
                )
                if barrio_salud == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    salud_barrio1 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    salud_barrio2 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    salud_barrio3 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    salud_barrio4 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
#Función Social 2
#Exactamente igual que la función Social 1, todo está igual, hasta los tabs.
elif select == "Caring":
    tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
    with tab1:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')       
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Compañias de Bomberos</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = bomberos['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['13101','13102']
                bomberos_alameda = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                frecuencia = bomberos_alameda['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "bomberos",
                    label_visibility="collapsed",
                )
                if barrio == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['13125']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['13105']
                    bomberos_barrio2 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio2['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['13114']
                    bomberos_barrio3 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio3['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['13111']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab2:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(educ, "Establecimiento educacionales", "green", 'graduation-cap', 'green')
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-graduation-cap" style="color: green; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimiento educacionales</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = educ['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                educ_alameda = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = educ_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_educ = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "educ",
                    label_visibility="collapsed",
                )
                if barrio_educ == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    educ_barrio1 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    educ_barrio2 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    educ_barrio3 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    educ_barrio4 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab3:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')    
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-heartbeat" style="color: orange; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimientos de Salud</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = salud['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                salud_alameda = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = salud_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_salud = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "salud",
                    label_visibility="collapsed",
                )
                if barrio_salud == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    salud_barrio1 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    salud_barrio2 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    salud_barrio3 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    salud_barrio4 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
#Función Social 3
#Exactamente igual que la función Social 1, todo está igual, hasta los tabs.
elif select == "Living":
    tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
    with tab1:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')       
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Compañias de Bomberos</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = bomberos['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['13101','13102']
                bomberos_alameda = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                frecuencia = bomberos_alameda['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "bomberos",
                    label_visibility="collapsed",
                )
                if barrio == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['13125']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['13105']
                    bomberos_barrio2 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio2['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['13114']
                    bomberos_barrio3 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio3['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['13111']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab2:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(educ, "Establecimiento educacionales", "green", 'graduation-cap', 'green')
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-graduation-cap" style="color: green; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimiento educacionales</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = educ['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                educ_alameda = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = educ_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_educ = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "educ",
                    label_visibility="collapsed",
                )
                if barrio_educ == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    educ_barrio1 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    educ_barrio2 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    educ_barrio3 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    educ_barrio4 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab3:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')    
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-heartbeat" style="color: orange; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimientos de Salud</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = salud['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                salud_alameda = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = salud_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_salud = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "salud",
                    label_visibility="collapsed",
                )
                if barrio_salud == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    salud_barrio1 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    salud_barrio2 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    salud_barrio3 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    salud_barrio4 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
#Función Social 4
#Exactamente igual que la función Social 1, todo está igual, hasta los tabs.
elif select == "Enjoy":
    tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
    with tab1:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')       
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Compañias de Bomberos</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = bomberos['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['13101','13102']
                bomberos_alameda = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                frecuencia = bomberos_alameda['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "bomberos",
                    label_visibility="collapsed",
                )
                if barrio == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['13125']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['13105']
                    bomberos_barrio2 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio2['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['13114']
                    bomberos_barrio3 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio3['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['13111']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab2:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(educ, "Establecimiento educacionales", "green", 'graduation-cap', 'green')
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-graduation-cap" style="color: green; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimiento educacionales</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = educ['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                educ_alameda = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = educ_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_educ = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "educ",
                    label_visibility="collapsed",
                )
                if barrio_educ == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    educ_barrio1 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    educ_barrio2 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    educ_barrio3 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    educ_barrio4 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab3:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')    
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-heartbeat" style="color: orange; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimientos de Salud</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = salud['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                salud_alameda = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = salud_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_salud = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "salud",
                    label_visibility="collapsed",
                )
                if barrio_salud == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    salud_barrio1 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    salud_barrio2 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    salud_barrio3 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    salud_barrio4 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
#Función Social 5
#Exactamente igual que la función Social 1, todo está igual, hasta los tabs.
elif select == "Working":
    tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
    with tab1:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')       
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Compañias de Bomberos</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = bomberos['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['13101','13102']
                bomberos_alameda = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                frecuencia = bomberos_alameda['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "bomberos",
                    label_visibility="collapsed",
                )
                if barrio == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['13125']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['13105']
                    bomberos_barrio2 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio2['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['13114']
                    bomberos_barrio3 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio3['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['13111']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab2:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(educ, "Establecimiento educacionales", "green", 'graduation-cap', 'green')
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-graduation-cap" style="color: green; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimiento educacionales</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = educ['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                educ_alameda = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = educ_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_educ = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "educ",
                    label_visibility="collapsed",
                )
                if barrio_educ == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    educ_barrio1 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    educ_barrio2 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    educ_barrio3 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    educ_barrio4 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab3:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')    
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-heartbeat" style="color: orange; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimientos de Salud</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = salud['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                salud_alameda = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = salud_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_salud = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "salud",
                    label_visibility="collapsed",
                )
                if barrio_salud == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    salud_barrio1 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    salud_barrio2 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    salud_barrio3 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    salud_barrio4 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
#Función Social 6
#Exactamente igual que la función Social 1, todo está igual, hasta los tabs.
elif select == "Sociability":
    tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
    with tab1:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')       
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Compañias de Bomberos</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = bomberos['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['13101','13102']
                bomberos_alameda = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                frecuencia = bomberos_alameda['CUT_CUERPO'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "bomberos",
                    label_visibility="collapsed",
                )
                if barrio == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['13125']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['13105']
                    bomberos_barrio2 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio2['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['13114']
                    bomberos_barrio3 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio3['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['13111']
                    bomberos_barrio1 = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns_ala)]
                    frecuencia = bomberos_barrio1['CUT_CUERPO'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab2:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(educ, "Establecimiento educacionales", "green", 'graduation-cap', 'green')
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-graduation-cap" style="color: green; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimiento educacionales</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = educ['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                educ_alameda = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = educ_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_educ = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "educ",
                    label_visibility="collapsed",
                )
                if barrio_educ == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    educ_barrio1 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    educ_barrio2 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    educ_barrio3 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_educ == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    educ_barrio4 = educ[educ['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = educ_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
    with tab3:
        st.write("Aquí va a ir texto")
        with st.container():
            col1, col2 = st.columns([0.8,0.2])
            with col1:
                st.write("Mapa Interactivo")
                m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
                add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')    
                folium.LayerControl(collapsed=False).add_to(m)
                st.title("Mapa interactivo")
                st_data = stf.st_folium(m, width=1000, height=700)
            with col2:
                st.markdown('### Legend')
                st.markdown('#### Elements')
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <span style="display: inline-flex; align-items: center;">
                        <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
                        width: 20px; height: 20px;"></span>
                        <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
                    </span>
                    """, unsafe_allow_html=True)
                st.markdown(f"""
                    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                    <span style="display: inline-flex; align-items: center;">
                        <i class="fas fa-heartbeat" style="color: orange; font-size: 20px;"></i>
                        <span style="padding-left: 5px;">Establecimientos de Salud</span>
                    </span>
                    """, unsafe_allow_html=True)
        st.write("")
        with st.container():
            col1, col2, col3 = st.columns([0.33,0.33,0.33])
            with col1:
                st.write("Zona Metropolitana de Santiago")
                st.write("")
                st.write("Gráfica")
                frecuencia = salud['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col2:
                st.write("Avenida Nueva Alameda")
                st.write("")
                st.write("Gráfica")
                columns_ala = ['EstaciÃ³n Central','Santiago','Cerrillos', 'Providencia']
                salud_alameda = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                frecuencia = salud_alameda['nom_comuna'].value_counts()
                st.bar_chart(frecuencia)
                st.write("Datos")
            with col3:
                barrio_salud = st.selectbox(
                    "Seleccione el barrio que desea ver",
                    ["Barrio 1", "Barrio 2", "Barrio 3", 
                    "Barrio 4"],
                    key = "salud",
                    label_visibility="collapsed",
                )
                if barrio_salud == "Barrio 1":
                    st.write("Gráfica")
                    columns_ala = ['EstaciÃ³n Central']
                    salud_barrio1 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio1['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 2":
                    st.write("Gráfica")
                    columns_ala = ['Santiago']
                    salud_barrio2 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio2['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 3":
                    st.write("Gráfica")
                    columns_ala = ['Cerrillos']
                    salud_barrio3 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio3['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
                elif barrio_salud == "Barrio 4":
                    st.write("Gráfica")
                    columns_ala = ['Providencia']
                    salud_barrio4 = salud[salud['nom_comuna'].apply(lambda x: x in columns_ala)]
                    frecuencia = salud_barrio4['nom_comuna'].value_counts()
                    st.bar_chart(frecuencia)
                    st.write("Datos")
        st.write("")
        st.write("Imágen/Texto")
