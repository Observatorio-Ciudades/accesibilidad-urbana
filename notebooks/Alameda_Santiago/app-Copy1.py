#Código GeoVisor 
import geopandas as gpd
import folium
from folium.plugins import FeatureGroupSubGroup
import streamlit as st
import streamlit_folium as stf
from folium import GeoJson
import matplotlib.pyplot as plt



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

# Selección de pestaña
option = st.selectbox(
    "Seleccione la pestaña",
    ["Resumen del índice", "Indicador 1 (Bomberos)", "Indicador 2 (Educación)", 
     "Indicador 3 (Salud)"]
)

# Mostrar contenido basado en la selección
if option == "Resumen del índice":
    st.title('Estadísticas del índice que se va a calcular')
    st.write("""
    Esta por definirse la forma en la que se presentará.
    """)
    with st.container():
        col1, col2= st.columns([0.99999, 0.00001])
        with col1:
            select = st.selectbox(
                "Seleccione la función social",
                ["Supplies", "Caring", "Living", 
                "Enjoy", "Working", "Sociability"]
            )
            if select == "Supplies":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1:
                    def read_file(filepath):
                        return gpd.read_file(filepath).to_crs('EPSG:4326')
                    dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/'
                    buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
                    bomberos = read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp')
                    columns = ['13102','13112','13115','13125','13105','13101', '13108','13111','13114']
                    bomberos = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns)]
                    poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp')
                    st.write("Aquí va a ir texto")
                    with st.container():
                        col1, col2 = st.columns([0.8,0.2])
                        with col1:
                            st.write("Mapa Interactivo")

                            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)

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
        
        
                            add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                            add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                            add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')

        
       
                            folium.LayerControl(collapsed=False).add_to(m)
        
                
                            #st.title("Mapa interactivo")
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
                            #st.bar_chart(frecuencia)
                            plt.figure(figsize=(10,6))
                            plt.bar(frecuencia.index, frecuencia.values)
                            plt.title('Frecuencia de Bomberos por Código Único Territorial')
                            plt.xlabel('Código Único Territorial')
                            plt.ylabel('Frecuencia')
                            plt.xticks(rotation=90)
                            st.pyplot(plt)
                            st.write("")
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
                                label_visibility="collapsed",
                            )
                            if barrio == "Barrio 1":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 2":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 3":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 4":
                                st.write("Gráfica")
                                st.write("Datos")
                    st.write("")
                    st.write("Imágen/Texto")
            elif select == "Caring":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1:
                    st.write("Aquí va a ir texto")
                    with st.container():
                        col1, col2 = st.columns([0.8,0.2])
                        with col1:
                            st.write("Mapa")
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
                            st.write("Datos")
                        with col2:
                            st.write("Avenida Nueva Alameda")
                            st.write("")
                            st.write("Gráfica")
                            st.write("Datos")
                        with col3:
                            barrio = st.selectbox(
                                "Seleccione el barrio que desea ver",
                                ["Barrio 1", "Barrio 2", "Barrio 3", 
                                "Barrio 4"],
                                label_visibility="collapsed",
                            )
                            if barrio == "Barrio 1":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 2":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 3":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 4":
                                st.write("Gráfica")
                                st.write("Datos")
                    st.write("")
                    st.write("Imágen/Texto")
            elif select == "Living":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1:
                    st.write("Aquí va a ir texto")
                    with st.container():
                        col1, col2 = st.columns([0.8,0.2])
                        with col1:
                            st.write("Mapa")
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
                            st.write("Datos")
                        with col2:
                            st.write("Avenida Nueva Alameda")
                            st.write("")
                            st.write("Gráfica")
                            st.write("Datos")
                        with col3:
                            barrio = st.selectbox(
                                "Seleccione el barrio que desea ver",
                                ["Barrio 1", "Barrio 2", "Barrio 3", 
                                "Barrio 4"],
                                label_visibility="collapsed",
                            )
                            if barrio == "Barrio 1":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 2":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 3":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 4":
                                st.write("Gráfica")
                                st.write("Datos")
                    st.write("")
                    st.write("Imágen/Texto")
            elif select == "Enjoy":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1:
                    st.write("Aquí va a ir texto")
                    with st.container():
                        col1, col2 = st.columns([0.8,0.2])
                        with col1:
                            st.write("Mapa")
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
                            st.write("Datos")
                        with col2:
                            st.write("Avenida Nueva Alameda")
                            st.write("")
                            st.write("Gráfica")
                            st.write("Datos")
                        with col3:
                            barrio = st.selectbox(
                                "Seleccione el barrio que desea ver",
                                ["Barrio 1", "Barrio 2", "Barrio 3", 
                                "Barrio 4"],
                                label_visibility="collapsed",
                            )
                            if barrio == "Barrio 1":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 2":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 3":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 4":
                                st.write("Gráfica")
                                st.write("Datos")
                    st.write("")
                    st.write("Imágen/Texto")
            elif select == "Working":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1:
                    st.write("Aquí va a ir texto")
                    with st.container():
                        col1, col2 = st.columns([0.8,0.2])
                        with col1:
                            st.write("Mapa")
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
                            st.write("Datos")
                        with col2:
                            st.write("Avenida Nueva Alameda")
                            st.write("")
                            st.write("Gráfica")
                            st.write("Datos")
                        with col3:
                            barrio = st.selectbox(
                                "Seleccione el barrio que desea ver",
                                ["Barrio 1", "Barrio 2", "Barrio 3", 
                                "Barrio 4"],
                                label_visibility="collapsed",
                            )
                            if barrio == "Barrio 1":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 2":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 3":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 4":
                                st.write("Gráfica")
                                st.write("Datos")
                    st.write("")
                    st.write("Imágen/Texto")
            elif select == "Sociability":
                tab1, tab2, tab3, tab4 = st.tabs(["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"])
                with tab1:
                    st.write("Aquí va a ir texto")
                    with st.container():
                        col1, col2 = st.columns([0.8,0.2])
                        with col1:
                            st.write("Mapa Interactivo")
                            def read_file(filepath):
                                return gpd.read_file(filepath).to_crs('EPSG:4326')

                            dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/'
                            buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
                            bomberos = read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp')
                            columns = ['13523','13522','13421','13404','13620','13503','13216','13613','13609','13102',
                                    '13206','13424','13610','13112','13115','13125','13105','13519','13507','13101',
                                    '13108','13111','13114','13318','13317']
                            bomberos = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns)]
                            poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp')
                            count = len(bomberos['OBJECTID_1'].unique())

                            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)

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
        
        
                            add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
                            add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
                            add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')

        
       
                            folium.LayerControl(collapsed=False).add_to(m)
        
                
                            st.title("Mapa interactivo")
                            st_data = stf.st_folium(m, width=1300, height=1000)
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
                            st.write("Datos")
                        with col2:
                            st.write("Avenida Nueva Alameda")
                            st.write("")
                            st.write("Gráfica")
                            st.write("Datos")
                        with col3:
                            barrio = st.selectbox(
                                "Seleccione el barrio que desea ver",
                                ["Barrio 1", "Barrio 2", "Barrio 3", 
                                "Barrio 4"],
                                label_visibility="collapsed",
                            )
                            if barrio == "Barrio 1":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 2":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 3":
                                st.write("Gráfica")
                                st.write("Datos")
                            elif barrio == "Barrio 4":
                                st.write("Gráfica")
                                st.write("Datos")
                    st.write("")
                    st.write("Imágen/Texto")
        with col2:
            st.write("")

elif option == "Indicador 1 (Bomberos)":
    st.title('Geovisor Eje Nueva Alameda con establecimientos de Bomberos')
    st.write("""
    Mapa interactivo del Eje Nueva Alameda con los establecimientos de bomberos.
    """)
    with st.container():
        col1, col2 = st.columns([4, 1])
        
        with col1:
            def read_file(filepath):
                return gpd.read_file(filepath).to_crs('EPSG:4326')

            dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/'
            buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
            bomberos = read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp')
            columns = ['13523','13522','13421','13404','13620','13503','13216','13613','13609','13102',
                    '13206','13424','13610','13112','13115','13125','13105','13519','13507','13101',
                    '13108','13111','13114','13318','13317']
            bomberos = bomberos[bomberos['CUT_CUERPO'].apply(lambda x: x in columns)]
            poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp')
            count = len(bomberos['OBJECTID_1'].unique())

            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)

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
        
        
            add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
            add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
            add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')

        
       
            folium.LayerControl(collapsed=False).add_to(m)
        
                
            st.title("Mapa interactivo")
            st_data = stf.st_folium(m, width=1300, height=1000)

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
            st.markdown(f"""
                <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
                <span style="display: inline-flex; align-items: center;">
                    <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
                    <span style="padding-left: 5px;"># de establecimientos: {count}</span>
                </span>
                """, unsafe_allow_html=True)

elif option == "Indicador 2 (Educación)":
    st.title('Geovisor Eje Nueva Alameda con establecimientos de Educación')
    st.write("""
    Mapa interactivo del Eje Nueva Alameda con los establecimientos de educación.
    """)
    with st.container():
        col1, col2 = st.columns([4, 1])
        
        with col1:
            def read_file(filepath):
                return gpd.read_file(filepath).to_crs('EPSG:4326')
        
           
            dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/'
            buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
            educ = read_file(dir + 'Establecimientos educacionales/establecimientos_educacionales_2021.shp')
            poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp')
        
            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
        
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
        
        
            add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
            add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
            add_gdf_marker(educ, "Establecimientos educacionales", "red", 'graduation-cap', 'green')
        
       
            folium.LayerControl(collapsed=False).add_to(m)
        
                
            st.title("Mapa interactivo")
            st_data = stf.st_folium(m, width=1300, height=1000)
        
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
                    <span style="padding-left: 5px;">Establecimientos educacionales</span>
                </span>
                """, unsafe_allow_html=True)

elif option == "Indicador 3 (Salud)":
    st.title('Geovisor Eje Nueva Alameda con establecimientos de Salud')
    st.write("""
    Mapa interactivo del Eje Nueva Alameda con los establecimientos de salud.
    """)
    with st.container():
        col1, col2 = st.columns([4, 1])
            
        with col1:
            def read_file(filepath):
                return gpd.read_file(filepath).to_crs('EPSG:4326')
        
            # Leer los archivos necesarios
            dir = 'OneDrive/Documentos/GitHub/accesibilidad-urbana/data/external/Proyecto Alameda Santiago/'
            buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
            salud = read_file(dir + 'Establecimientos salud/establec_salud_14_mayo_2021.shp')
            poly_santiago = read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp')
        
            m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
        
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
                
                
            add_gdf_to_map(buffer, "Buffer Avenida Alameda", "blue")
            add_gdf_to_map(poly_santiago, "Polígono de Santiago", "green")
            add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')
                
                # Agregar control de capas
            folium.LayerControl(collapsed=False).add_to(m)
        
                
            st.title("Mapa Interactivo")
            st_data = stf.st_folium(m, width=1300, height=1000)
            

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


