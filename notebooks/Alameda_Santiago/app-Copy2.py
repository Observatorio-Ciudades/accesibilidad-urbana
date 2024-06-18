#Código GeoVisor 

### Librerías ###
import geopandas as gpd
import folium
from folium.plugins import FeatureGroupSubGroup
import streamlit as st
import streamlit_folium as stf
from folium import GeoJson
import matplotlib.pyplot as plt

### Funciones ###
def read_file(filepath):
    return gpd.read_file(filepath).to_crs('EPSG:4326')

def add_gdf_to_map(gdf, name, color):
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

    ).add_to(g)

def add_gdf_marker(gdf, name, color, icon_name, icon_color):
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
    ).add_to(g)
    for _, row in gdf.iterrows():
        lat = row.geometry.y
        lon = row.geometry.x
            
        folium.Marker(
            location=[lat, lon],
            icon=folium.Icon(color=icon_color, prefix='fa', icon=icon_name)
        ).add_to(g)




### Variables de entrada ###
dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/'
buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp')
bomberos = read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp')
columns = ['13102','13112','13115','13125','13105','13101', '13108','13111','13114']
bomberos = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns)]
salud = read_file(dir + 'Establecimientos salud/establec_salud_14_mayo_2021.shp')
salud = salud[salud['nom_provin'] == 'Santiago'].sample(n=100, random_state=43)
educ = read_file(dir + 'Establecimientos educacionales/establecimientos_educacionales_2021.shp')
educ = educ[educ['nom_provin'] == 'Santiago'].sample(n=100, random_state=43)

# Configuración inicial de Streamlit
st.set_page_config(
    page_title='Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile',
    page_icon=":wine_glass:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título y descripción
st.title('Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile')
st.write("Geovisor con la información más relevante de la zona")

select = st.selectbox(
    "Seleccione la función social",
    ["Supplies", "Caring", "Living", 
    "Enjoy", "Working", "Sociability"]
)
#Función Social 1
if select == "Supplies":
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
#Función Social 2
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
