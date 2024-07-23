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
from streamlit_folium import st_folium
from folium.plugins import FeatureGroupSubGroup


### Funciones ###
#Función para leer los archivos gpd y cambiarles el formato de coordenada al que usamos siempre
def read_file(filepath):
    return gpd.read_file(filepath).to_crs('EPSG:4326') 
    
#Función para agregar un geodataframe como capa al mapa interactivo
def add_gdf_to_map(gdf, name, color, m): #Nombre del geodataframe que se va a mostrar, nombre que se va a mostrar en el mapa, color que se va a presentar
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

def add_gdf_marker(gdf, name, color, icon_name, icon_color, m): #Este es igual que el anterior pero se generó para los que son íconos en este mapa, ósea los bomberos, salud y educación.
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

### Variables de entrada ###
dir_grl = "/home/jovyan/accesibilidad-urbana/data/external" #El directorio de donde tengo guardado la carpeta de toda la data
buffer = read_file("alameda_buffer800m_gcs_v1.geojson") #Data del buffer de la Nueva Alameda
hexas_santiago = read_file('santiago_hexanalysis_res8_4_5_kmh.geojson') #Data de los hexágonos. :p
comunas_santi = read_file("santiago_comunasanalysis_4_5_kmh.geojson") #Data de las comunas.
unidades_vecinales = read_file("santiago_unidadesvecinalesanalysis_4_5_kmh.geojson") #Data de las unidades vecinales
#hex_schema = "projects_research"
#hex_table = "santiago_hexproximity_hqsl_4_5_kmh"
#n = '9'
#query = f'SELECT * FROM {hex_schema}.{hex_table} WHERE \"res\" = {n}'
#SPYDER = aup.gdf_from_query(query, geometry_col='geometry') ### No pude agregar esto porque no tengo docker por el trojano, entonces descargue la base de datos ###
spyderplot = read_file('santiago_hexanalysis_res8_4_5_kmh.geojson') #Data de las 6 Funciones sociales y columna geometría
santiago = spyderplot.drop(columns = "geometry") #Data sin columna Geometry
columns_santiago = santiago.columns #Columnas y valores

#Datas de prueba para la función de diferentes usuarios:
bomberos = read_file(dir_grl + '/Bomberos/layer_companias_de_bomberos/layer_companias_de_bomberos_20231110080349.shp') #Data de los bomberos Chile Nación
#columns = ['13102','13112','13115','13125','13105','13101', '13108','13111','13114'] #Códigos postales de la zona metropolitana Santiago
bomberos = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns)] #Filtrar solo los bomberos de la metropolitana Santiago
salud = read_file(dir_grl + '/capas_pois/salud/establec_salud_14_mayo_2021.shp') #Data Salud Metro Santiago
salud = salud[salud['nom_provin'] == 'Santiago'].sample(n=100, random_state=43) #Filtrar solo 100 puntos de forma aleatoria para la visualización más rápida del mapa
educ = read_file(dir_grl + '/capas_pois/educativo/layer_establecimientos_de_educacion_superior_20220309024111.shp') #Data educ metro Santiago
educ = educ[educ['COD_REGION'] == 13].sample(n=100, random_state=43) #Filtrar solo 100 puntos de forma aleatoria para la visualización más rápida del mapa

def mapas():
    user_tabs = ["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"]
    selected_user = st.selectbox("Seleccione el usuario", user_tabs)

    if selected_user == "Usuario 1":
        mapa_usuario_1()
    elif selected_user == "Usuario 2":
        mapa_usuario_2()
    elif selected_user == "Usuario 3":
        mapa_usuario_3()
    elif selected_user == "Usuario 4":
        mapa_usuario_4()

spyderplot = read_file('santiago_hexanalysis_res8_4_5_kmh.geojson') #Data de las 6 Funciones sociales y columna geometría
santiago = spyderplot.drop(columns = "geometry") #Data sin columna Geometry
columns_santiago = santiago.columns #Columnas y valores

santiago_comunas = comunas_santi.drop(columns = "geometry") #Data sin columna Geometry
columns_comunas = santiago_comunas.columns #Columnas y valores

santiago_unidades = comunas_santi.drop(columns = "geometry") #Data sin columna Geometry
columns_unidades = santiago_comunas.columns #Columnas y valores

def scatters():
    with st.container(): #Depues de generar el container del mapa, justo abajo se genera otro container pero ahora para las gráficas.
        col1, col2, col3 = st.columns([0.33,0.33,0.33]) #Como tenemos gráficas para Santiago, Alameda y Barrios, se divide en 3.
        with col1:
            st.write("Zona Metropolitana de Santiago")
            st.write("Gráfica")
            column_sums = santiago.sum() #Se suman los valores de cada una de las columnas.
            labels = columns_santiago #Se agarran los nombres de las columnas para labels de la gráfica
            sums = column_sums.values #Se obtienen los valores numéricos de la suma

            fig = go.Figure() #Se genera el plot con la función 'go' de Plotly
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself'
                fillcolor = "orchid"
                line_color = 'salmon'
                )) #Se genera el spyderplot con popups. Se muestra la suma en la variable 'r' y los títulos en la variable 'theta'. Se pueden cambiar estas variables y se vería el cambio en la gráfica.

                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                        visible=False,
                        range=[0, max(sums)]
                        )),
                    showlegend=False
                        ) #Se genera el layout del grid de la gráfica
                st.plotly_chart(fig) #Se muestra en la app
                st.write("Datos") 
        with col2:
            comunas = comunas_santi.loc[comunas_santi.loc["name"] == "hsql"].loc[0]
            st.write("Comunas de Santiago")
            st.write("Gráfica")
            selecciona_comunas = st.selectbox("Seleccione una comuna:", [comuna["name"]])
            comuna_selected = comunas_santi[comunas_santi["name"] == selecciona_comunas]
            column_sums_comunas = santiago_comunas.sum() #Se suman los valores de cada una de las columnas.
            labels = columns_comunas #Se agarran los nombres de las columnas para labels de la gráfica
            sums = column_sums_comunas.values #Se obtienen los valores numéricos de la suma

            fig = go.Figure() #Se genera el plot con la función 'go' de Plotly
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself'
                fillcolor = "orchid"
                line_color = 'salmon'
                )) #Se genera el spyderplot con popups. Se muestra la suma en la variable 'r' y los títulos en la variable 'theta'. Se pueden cambiar estas variables y se vería el cambio en la gráfica.

                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                        visible=False,
                        range=[0, max(sums)]
                        )),
                    showlegend=False
                        ) #Se genera el layout del grid de la gráfica
                st.plotly_chart(fig) #Se muestra en la app
                st.write("Datos") 
        with col3:
            unidades = unidades_vecinales.loc[unidades_vecinales.loc["name"] == "COD_UNICO_"].loc[0]
            st.write("Unidades Vecinales")
            st.write("Gráfica")
            selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", [unidades["name"]])
            unidad_selected = unidades_vecinales[unidades_vecinales["name"] == selecciona_unidades]
            column_sums_unidades = santiago_unidades.sum() #Se suman los valores de cada una de las columnas.
            labels = columns_unidades #Se agarran los nombres de las columnas para labels de la gráfica
            sums = column_sums_comunas.values #Se obtienen los valores numéricos de la suma

            fig = go.Figure() #Se genera el plot con la función 'go' de Plotly
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself'
                fillcolor = "orchid"
                line_color = 'salmon'
                )) #Se genera el spyderplot con popups. Se muestra la suma en la variable 'r' y los títulos en la variable 'theta'. Se pueden cambiar estas variables y se vería el cambio en la gráfica.

                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                        visible=False,
                        range=[0, max(sums)]
                        )),
                    showlegend=False
                        ) #Se genera el layout del grid de la gráfica
                st.plotly_chart(fig) #Se muestra en la app
                st.write("Datos") 

#Función para determinar el nivel basado en el valor
def get_level_text(value):
    if value < 11:
        return "Muy Bajo"
    elif value < 23:
        return "Bajo"
    elif value < 35:
        return "Medio"
    elif value < 57:
        return "Alto"
    else:
        return "Muy Alto"

# Función para crear el gráfico de indicador
def create_gauge_chart(title, value):
    level_text = get_level_text(value)
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 33}},
        #number={'font': {'size': 40}},
        gauge={
            "axis": {"range": [0, 60], 'tickcolor': "black"},
            'bar': {'color': "darkslategray"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "olive",
            "steps": [
                {'range': [0, 11], 'color': "red"},
                {'range': [11, 23], 'color': "orange"},
                {'range': [23, 35], 'color': "gold"},
                {'range': [35, 47], 'color': "greenyellow"},
                {'range': [47, 60], 'color': "springgreen"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': value
            }
        }
    ))

    fig.update_layout(paper_bgcolor = "whitesmoke", font = {'color': "black", 'family': "Arial"})
    # Añadir anotación para el texto del nivel
    fig.add_annotation(
        x=0.5,
        y=0.3,
        text=level_text,
        showarrow=False,
        font=dict(size=64),
        xref="paper",
        yref="paper",
        xanchor="center",
        yanchor="middle"
    )
    return fig

# Filtrar los datos para "alameda", "comunas" y "unidades vecinales" para los gauges plots.
alameda_data = buffer.loc[buffer['name'] == "alameda"].iloc[0]
comunas_data =  comunas_santi.loc[comunas_santi["name"] == "hsql"].iloc[0]
unidades_vecinales_data = unidades_vecinales.loc[unidades_vecinales["name"] == "COD_UNICO_"]

def gauges():
    with st.container(): 
        col1, col2, col3 = st.columns([0.33,0.33,0.33]) 
        with col1:
            select_gauge = st.selectbox(
                "Seleccione la característica a analizar",
                ["Sociability", "Wellbeing", "Environmental Impact"]
            )

            # Crear el contenedor de Streamlit
            if select_gauge == "Sociability":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1: 
                    st.write("Medición de 'Sociability' para Usuario 1")
                    sociability_fig = create_gauge_chart("Sociability", comunas_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab2:
                    st.write("Medición de 'Sociability' para Usuario 2")
                    sociability_fig = create_gauge_chart("Sociability", alameda_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab3:
                    st.write("Medición de 'Sociability' para Usuario 3")
                    sociability_fig = create_gauge_chart("Sociability", alameda_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab4:
                    st.write("Medición de 'Sociability' para Usuario 4")
                    sociability_fig = create_gauge_chart("Sociability", alameda_data["sociability"])
                    st.plotly_chart(sociability_fig)
            elif select_gauge == "Wellbeing":
                wellbeing_fig = create_gauge_chart("Wellbeing", alameda_data["wellbeing"])
                st.plotly_chart(wellbeing_fig)
            else:
                environmental_impact_fig = create_gauge_chart("Environmental Impact", alameda_data["environmental_impact"])
                st.plotly_chart(environmental_impact_fig)
        with col2:
            selecciona_comunas = st.selectbox("Seleccione una comuna:", comunas_santi["name"].unique())
            comuna_selected = comunas_santi[comunas_santi["name"] == selecciona_comunas]
            select_gauge = st.selectbox(
                "Seleccione la característica a analizar",
                ["Sociability", "Wellbeing", "Environmental Impact"]
            )

            # Crear el contenedor de Streamlit
            if select_gauge == "Sociability":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1: 
                    st.write("Medición de 'Sociability' para Usuario 1")
                    sociability_fig = create_gauge_chart("Sociability", comunas_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab2:
                    st.write("Medición de 'Sociability' para Usuario 2")
                    sociability_fig = create_gauge_chart("Sociability", comunas_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab3:
                    st.write("Medición de 'Sociability' para Usuario 3")
                    sociability_fig = create_gauge_chart("Sociability", comunas_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab4:
                    st.write("Medición de 'Sociability' para Usuario 4")
                    sociability_fig = create_gauge_chart("Sociability", comunas_data["sociability"])
                    st.plotly_chart(sociability_fig)
            elif select_gauge == "Wellbeing":
                wellbeing_fig = create_gauge_chart("Wellbeing", comunas_data["wellbeing"])
                st.plotly_chart(wellbeing_fig)
            else:
                environmental_impact_fig = create_gauge_chart("Environmental Impact", comunas_data["environmental_impact"])
                st.plotly_chart(environmental_impact_fig)
        with col3:
            selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["name"].unique())
            unidad_selected = unidades_vecinales[unidades_vecinales["name"] == selecciona_unidades]
            select_gauge = st.selectbox(
                "Seleccione la característica a analizar",
                ["Sociability", "Wellbeing", "Environmental Impact"]
            )

            # Crear el contenedor de Streamlit
            if select_gauge == "Sociability":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1: 
                    st.write("Medición de 'Sociability' para Usuario 1")
                    sociability_fig = create_gauge_chart("Sociability", unidades_vecinales_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab2:
                    st.write("Medición de 'Sociability' para Usuario 2")
                    sociability_fig = create_gauge_chart("Sociability", unidades_vecinales_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab3:
                    st.write("Medición de 'Sociability' para Usuario 3")
                    sociability_fig = create_gauge_chart("Sociability", unidades_vecinales_data["sociability"])
                    st.plotly_chart(sociability_fig)
                with tab4:
                    st.write("Medición de 'Sociability' para Usuario 4")
                    sociability_fig = create_gauge_chart("Sociability", unidades_vecinales_data["sociability"])
                    st.plotly_chart(sociability_fig)
            elif select_gauge == "Wellbeing":
                wellbeing_fig = create_gauge_chart("Wellbeing", unidades_vecinales_data["wellbeing"])
                st.plotly_chart(wellbeing_fig)
            else:
                environmental_impact_fig = create_gauge_chart("Environmental Impact", unidades_vecinales_data["environmental_impact"])
                st.plotly_chart(environmental_impact_fig)

    
def mapa_usuario_1():
    with st.container():
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.write("Mapa Interactivo Usuario 1")
            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45, tiles="cartodb positron")
            add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue", m)
            add_gdf_to_map(hexas_santiago, "Polígono de Santiago", "green", m)
            folium.LayerControl(collapsed=False).add_to(m)
            st.title("Mapa interactivo")
            st_folium(m, width=2000, height=700)
            scatters() #Llama a la función de gráficas scatter para que aparezcan justo debajo del mapa.
            gauges()
        with col2:
            mostrar_legenda()

def mapa_usuario_2():
    with st.container():
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.write("Mapa Interactivo Usuario 2")
            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45, tiles="cartodb positron")
            add_gdf_to_map(salud, "Otro Buffer", "purple", m)
            add_gdf_to_map(hexas_santiago, "Otro Polígono", "orange", m)
            folium.LayerControl(collapsed=False).add_to(m)
            st.title("Mapa interactivo")
            st_folium(m, width=2000, height=700)
            scatters() #Llama a la función de gráficas scatter para que aparezcan justo debajo del mapa.
            gauges()
        with col2:
            mostrar_legenda()

def mapa_usuario_3():
    col1, col2 = st.columns([0.8, 0.2])
    with col1:
        st.write("Mapa Interactivo Usuario 2")
        m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45, tiles="cartodb positron")
        add_gdf_to_map(comunas_santi, "Otro Buffer", "forestgreen", m)
        add_gdf_to_map(hexas_santiago, "Otro Polígono", "indianred", m)
        folium.LayerControl(collapsed=False).add_to(m)
        st.title("Mapa interactivo")
        st_folium(m, width=2000, height=700)
        scatters() #Llama a la función de gráficas scatter para que aparezcan justo debajo del mapa.
        gauges()
    with col2:
            mostrar_legenda()

def mapa_usuario_4():
    col1, col2 = st.columns([0.8, 0.2])
    with col1:
        st.write("Mapa Interactivo Usuario 2")
        m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45, tiles="cartodb positron")
        add_gdf_to_map(unidades_vecinales, "Otro Buffer", "maroon", m)
        add_gdf_to_map(hexas_santiago, "Otro Polígono", "navy", m)
        folium.LayerControl(collapsed=False).add_to(m)
        st.title("Mapa interactivo")
        st_folium(m, width = 2000, height = 700)
        scatters() #Llama a la función de gráficas scatter para que aparezcan justo debajo del mapa.
        gauges()
    with col2:
            mostrar_legenda()

def mostrar_legenda():
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

# Ejemplo de como llamarlo en el script principal
mapas()
