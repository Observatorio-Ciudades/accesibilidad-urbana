## Librerías ###
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from streamlit_folium import folium_static, st_folium
import folium
from folium.plugins import FeatureGroupSubGroup
from folium import GeoJson
import geopandas as gpd

# Estilos para ocultar el botón de pantalla completa
hide_img_fs = '''
            <style>
            button[title="View fullscreen"]{
                visibility: hidden;}
            </style>
            '''

# Función para leer los archivos gpd y cambiarles el formato de coordenada
def read_file(filepath):
    return gpd.read_file(filepath).to_crs('EPSG:4326') 

# Configuración inicial de la página de Streamlit
st.set_page_config(
    page_title="Proyecto Volvo",
    page_icon=":car",
    layout="wide",
    initial_sidebar_state="collapsed",
)

if "map" not in st.session_state:
    st.session_state.map = None
    st.session_state.legend = None

# Portada

# Remove header space

st.markdown(""" 
    <style>
    
           /* Remove blank space at top and bottom */ 
           .block-container {
               padding-top: 0rem;
               padding-bottom: 0rem;
            }
           
           /* Remove blank space at the center canvas */ 
           .st-emotion-cache-z5fcl4 {
               position: relative;
               top: -62px;
               }
           
           /* Make the toolbar transparent and the content below it clickable */ 
           .st-emotion-cache-18ni7ap {
               pointer-events: none;
               background: rgb(255 255 255 / 0%)
               }
           .st-emotion-cache-zq5wmm {
               pointer-events: auto;
               background: rgb(255 255 255);
               border-radius: 5px;
               }
    </style>
    """, unsafe_allow_html=True) #Hacer el header más estrecho.

# Banner
url = "https://raw.githubusercontent.com/Observatorio-Ciudades/NuevaAlameda/main/output/figures/Header_un_cuarto.jpg"
st.image(url, use_container_width=True)

# Sidebar
st.sidebar.title("Instructivo de uso")
st.sidebar.markdown(
    """
    - [Datos](#datos)
    - [Visualizaciones](#visualizaciones)
    - [Mapas](#mapas)
    """
)

# Descripción de la página
st.markdown("""
         Insertar descripción del proyecto aquí.
         """)

tab1, tab2 = st.tabs(["Guadalajara", "Medellín"])
with tab1:
    #@st.cache_data
    st.markdown("""
        ### Guadalajara
        Insertar descripción de la ciudad aquí.
        """)
    # Agregar un mapa interactivo de la ciudad de México
    m = folium.Map(location=[20.659698, -103.349609], zoom_start=12)
    folium.Marker(location=[20.659698, -103.349609], popup="Guadalajara").add_to(m)
    folium_static(m, width=1200, height=600)

with tab2:
    #@st.cache_data
    st.markdown("""
        ### Medellín
        Insertar descripción de la ciudad aquí.
        """)
    # Agregar un mapa interactivo de Medellín
    m = folium.Map(location=[6.2442, -75.5812], zoom_start=12)
    folium.Marker(location=[6.2442, -75.5812], popup="Medellín").add_to(m)
    folium_static(m, width=1200, height=600)


def user_area_selection():
    st.markdown(
        """
        ### Selección de área de usuario
        Selecciona un área de interés en el mapa.
        """)
    # Columnas
    col1, col2, col3 = st.columns([0.30, 0.30, 0.30])
    
    # Columna 1
    with col1:
        st.write("### Área de usuario")
    
    # Columna 2
    with col2:
        st.write("### Área de usuario")
        seleccionador_1 = st.selectbox(
            label = "Columna de selección", label_visibility = "collapsed",
            options = ["Localidad 1", "Localidad 2", "Localidad 3"],
            placeholder = "Selecciona una localidad",
            help = "Selecciona una localidad de la lista desplegable",
            key = "selector_1"
            )
        
    # Columna 3
    with col3:
        st.write("### Área de usuario")
        seleccionador_2 = st.selectbox(
            label = "Columna de selección", label_visibility = "collapsed",
            options = ["Localidad 1", "Localidad 2", "Localidad 3"],
            placeholder = "Selecciona una localidad",
            help = "Selecciona una localidad de la lista desplegable",
            key = "selector_2"
            )
    
    # Guardar la selección del usuario  
    st.session_state.seleccionador_1 = seleccionador_1
    st.session_state.seleccionador_2 = seleccionador_2

    return seleccionador_1, seleccionador_2

# def user_indicator_selection():
#     st.markdown(
#         """
#         ### Selección de indicador
#         Selecciona un indicador de interés.
#         """)
#     # Columnas
#     col1, _, _ = st.columns([0.30, 0.30, 0.30])
    
#     # Columna 1
#     with col1:
#         st.write("### Indicador")
#         column_to_plot = st.selectbox(
#             label = "Columna de selección", label_visibility = "collapsed",
#             options = ["Indicador 1", "Indicador 2", "Indicador 3"],
#             placeholder = "Selecciona un indicador",
#             help = "Selecciona un indicador de la lista desplegable"
#             )
    
#     # Returning the selected indicator for further use
#     return column_to_plot
    

# Simular un DataFrame ficticio
df_fake = pd.DataFrame({
    "Localidad 1": np.random.rand(50),
    "Localidad 2": np.random.rand(50),
    "Localidad 3": np.random.rand(50),
})

def scatters(seleccionador_1, seleccionador_2, column_to_plot):
    st.markdown(
        """
        ### Gráfico de dispersión
        Gráfico de dispersión de los indicadores seleccionados.
        """)
    if seleccionador_1 and seleccionador_2:
        # Guardar el estado de los selectores
        # Actualizar session_state solo si cambia la selección
        if("seleccionador_1" not in st.session_state or seleccionador_1 != st.session_state.seleccionador_1):
            st.session_state.seleccionador_1 = seleccionador_1
        if("seleccionador_2" not in st.session_state or seleccionador_2 != st.session_state.seleccionador_2):
            st.session_state.seleccionador_2 = seleccionador_2
            
            st.session_state.seleccionador_1 = seleccionador_1
            st.session_state.seleccionador_2 = seleccionador_2
            
        # Renderizar las gráficas siempre, independientemente de si cambian o no los selectores
        col1, col2, col3 = st.columns([0.25, 0.25, 0.25])
        with col1:
            st.write("### Gráfico de dispersión")
            # Crear un gráfico de dispersión
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_fake[seleccionador_1],
                y=df_fake[seleccionador_2],
                mode='markers',
                marker=dict(size=10, color='blue'),
                text=column_to_plot
            ))
            fig.update_layout(
                title="Gráfico de dispersión",
                xaxis_title=seleccionador_1,
                yaxis_title=seleccionador_2
            )
            st.plotly_chart(fig, use_container_width=True, key="scatter1")
            
        # Columna 2
        with col2:
            st.write("### Gráfico de dispersión")
            # Crear un gráfico de dispersión
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_fake[seleccionador_1],
                y=df_fake[seleccionador_2],
                mode='markers',
                marker=dict(size=10, color='blue'),
                text=column_to_plot
            ))
            fig.update_layout(
                title="Gráfico de dispersión",
                xaxis_title=seleccionador_1,
                yaxis_title=seleccionador_2
            )
            st.plotly_chart(fig, use_container_width=True, key="scatter2")
            
        # Columna 3
        with col3:
            st.write("### Gráfico de dispersión")
            # Crear un gráfico de dispersión
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_fake[seleccionador_1],
                y=df_fake[seleccionador_2],
                mode='markers',
                marker=dict(size=10, color='blue'),
                text=column_to_plot
            ))
            fig.update_layout(
                title="Gráfico de dispersión",
                xaxis_title=seleccionador_1,
                yaxis_title=seleccionador_2
            )
            st.plotly_chart(fig, use_container_width=True, key="scatter3")
    else:
        st.warning("Por favor, selecciona un área de usuario y un indicador para continuar.")
    
    
# Función para agregar un geodataframe como capa al mapa interactivo
def add_gdf_to_map(gdf, name, color, m, weight=1, 
                   fill_opacity=0, dashArray=None, show=False):
    g = FeatureGroupSubGroup(m, name, show=show)
    m.add_child(g)
    
    fields = [field for field in gdf.columns if field != 'geometry']
    
    GeoJson(
        gdf,
        zoom_on_click=True,
        style_function=lambda feature: {
            'fillColor': color,
            'color': 'black',
            'weight': weight,
            'fillOpacity': fill_opacity,
            'dashArray': None if dashArray is None else dashArray,
        },
        highlight_function=lambda x: {'weight': 5, 'color': 'white'},
        tooltip=folium.GeoJsonTooltip(fields=fields, labels=True, sticky=True)
    ).add_to(g)

def add_gdf_marker(gdf, name, color, icon_name, icon_color, m):
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
        
def mapas():
    # TEST fixed container
    custom_css = """
    <style>
    /* Style for the container with height limit */
    .fixed-height-container {
        height: 10px; /* Set your desired height limit */
        overflow: hidden; /* Prevent scrolling */
    }

    /* Optional: Style for the content inside the container */
    .fixed-height-content {
        width: 100%;    }
    </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)

    # with st.container():
    #     st.markdown('<div class="fixed-height-container"><div class="fixed-height-content">', 
    #                 unsafe_allow_html=True)
    #     col1, col2 = st.columns([0.88, 0.12])
    #     if st.session_state.map is None:
    #         with col1:
    #             m = mapa_usuario_1()
    #             st.session_state.map = m
    #             st.components.v1.html(m.render(), height=500)

    #         with col2:
    #             legend = mostrar_legenda()
    #             st.session_state.legend = legend
    #     else:
    #         with col1:
    #             st.components.v1.html(st.session_state.map.render(), height=500) #Optimizar el renderizado.
    #         with col2:
    #             mostrar_legenda()

    #     st.markdown('</div></div>', unsafe_allow_html=True)

# def mapa_usuario_1():
#     # f = folium.Figure(width=1800, height=500)
#     m = folium.Map(
#         location=[buffer.geometry.centroid.y.mean(), 
#         buffer.geometry.centroid.x.mean()],
#         zoom_start=12,
#         tiles="cartodb positron",
#     )

#     # Add HQSL choropleth to map
#     hqsl=folium.FeatureGroup(name='HQSL', show=True)
#     m.add_child(hqsl)
#     folium.Choropleth(
#             geo_data=hexas_santiago,
#             name="HQSL",
#             data=hexas_santiago,
#             key_on="feature.properties.fidx_txt",
#             columns=["fidx_txt", "class"],
#             fill_color="RdPu",
#             fill_opacity=0.5,
#             line_opacity=0.05,
#             legend_name="HQSL",
#         ).geojson.add_to(hqsl)
    
#     # Add calidad de espacio publico choropleth to map
#     cal_ep=folium.FeatureGroup(name='Calidad de espacio público', show=False)
#     m.add_child(cal_ep)
#     folium.Choropleth(
#             geo_data=calidad_ep,
#             name="Calidad de espacio público",
#             data=calidad_ep,
#             key_on="feature.properties.fidx_txt",
#             columns=["fidx_txt", "calidad_ep"],
#             fill_color="Spectral",
#             fill_opacity=0.75,
#             line_opacity=0,
#             legend_name="Calidad de espacio público",
#         ).geojson.add_to(cal_ep)
    


    
#     # add_gdf_to_map(hexas_santiago, "Hexágonos Análisis", "green", m)
    
#     add_gdf_to_map(uv_geom, "Unidades Vecinales", "red", m,
#                    weight=1, fill_opacity=0, show=False)
    
#     add_gdf_to_map(comunas_geom, "Comunas", "white", m,
#                    weight=2, fill_opacity=0, show=True)
    
#     add_gdf_to_map(buffer, "Nueva Alameda", "white", m,
#                      fill_opacity=0.5, weight=0.5, show=True)
    
#     # st.write("Mapa Interactivo Usuario 1")
#     # avoid page reload
#     folium.LayerControl(collapsed=False, position="topleft",).add_to(m)
#     # st_folium(m, width=1800, height=250, returned_objects=[]) # height 700
#     # Test for static map
#     # folium_static(m, width=1000, height=500) # height 700
#     # Test remove returned_objects
#     # st_map = st_folium(m, width=1800, height=500, returned_objects=[]) # height 700
    
#     # Working section
#     # st.components.v1.html(folium.Figure().add_child(m).render(), height=500)
    
    
#     # st.html("<style> .main {overflow: hidden} </style>")
#     #css = '''
#     #<style>
#     #section.main > div:has(~ footer ) {
#     #    padding-bottom: 5px;
#     #}
#     #</style>
#     #'''
#     #st.markdown(css, unsafe_allow_html=True)''
#     return folium.Figure().add_child(m)


# def mostrar_legenda():

#     # Add whitebox background
#     # Define the HTML and CSS for the white box
#     # Define the HTML and CSS for the background white box

#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')
#     st.markdown(' ')

#     st.markdown(
#     """
#     <style>
#         .centered-text {
#             text-align: center; /* Center the text horizontally */
#             font-size: 16px; /* Set the text size */
#             width: 20%; /* Ensure the text spans the entire container width */
#             font-weight: bold; /* Make the text bold */
#         }
#     </style>

#     <div class="centered-text">
#         Simbología
#     </div>
#     """,
#     unsafe_allow_html=True)

#     # st.markdown('#### Elements')
#     st.markdown(f"""
#         <span style="display: inline-flex; align-items: center;">
#             <span style="background-color: rgba(227, 227, 227, 0.5); border: 1px solid rgba(0, 0, 0, 1); display: inline-block;
#             width: 20px; height: 20px;"></span>
#             <span style="padding-left: 5px; font-size: 12px;">Nueva Alameda</span>
#         </span>
#         """, unsafe_allow_html=True)
#     st.markdown(f""" 
#         <span style="display: inline-flex; align-items: center;"> 
#             <span style="background-color: rgba(53, 202, 12, 0); border: 1px solid rgba(0, 0, 0, 1); display: inline-block;
#             width: 20px; height: 20px;"></span>
#             <span style="padding-left: 5px; font-size: 12px;">Unidades Vecinales</span>
#         </span>
#         """, unsafe_allow_html=True)
#     st.markdown(f"""
#         <span style="display: inline-flex; align-items: center;">
#             <span style="background-color: rgba(0, 99, 194, 0); border: 3px solid rgba(0, 0, 0, 1); display: inline-block;
#             width: 20px; height: 20px;"></span>
#             <span style="padding-left: 5px; font-size: 12px;">Comunas</span>
#         </span>
#         """, unsafe_allow_html=True)
    
#     # HQSL Choropleth legend
    
#     st.markdown(
#     """
#     <style>
#         .centered-text {
#             text-align: center; /* Center the text horizontally */
#             font-size: 12px; /* Set the text size */
#             width: 20%; /* Ensure the text spans the entire container width */
#             font-weight: bold; /* Make the text bold */
#         }
#     </style>

#     <div class="centered-text">
#         Calidad de Vida Social (HQSL)
#     </div>
#     """,
#     unsafe_allow_html=True
# )
#     st.markdown("""
#         <span style="display: inline-flex; align-items: center;">
#             <span style=" background: rgb(63,94,251);
#                 background: linear-gradient(0deg, rgba(254,235,226,1) 17%, 
#                 rgba(252,197,192,1) 34%, rgba(250,159,181,1) 51%, 
#                 rgba(247,104,161,1) 68%, 
#                 rgba(197,27,138,1) 85%, rgba(122,1,119,1) 100%);
#                 display: inline-block;
#             width: 20px; height: 60px;">
#         </span>
#                 <style>
#         .multiline-span {
#             display: inline-block; /* Allows span to have block-like behavior while remaining inline */
#             width: 200px; /* Set a width to control where the text wraps */
#                 margin-left: 10px; /* Add a left indent */
#                  font-size: 12px; /* Set the text size */
#         }
#     </style>
# </head>
# <body>
#     <span class="multiline-span">
#         Mayor<br>
#                 <br>
#         Menor
#     </span>
#         """, unsafe_allow_html=True)
    
#     # Calidad del espacio público legend
    
#     st.markdown(
#     """
#     <style>
#         .centered-text {
#             text-align: left; /* Center the text horizontally */
#             font-size: 12px; /* Set the text size */
#             width: 100%; /* Ensure the text spans the entire container width */
#             font-weight: bold; /* Make the text bold */
#             margin-bottom: 0; /* Remove spacing below the text */
#         }
#     </style>

#     <div class="centered-text">
#         Calidad del espacio público para la movilidad activa
#     </div>
#     """,
#     unsafe_allow_html=True
# )
#     st.markdown("""
#         <span style="display: inline-flex; align-items: center;">
#             <span style=" background: rgb(158,1,66);
#             background: linear-gradient(0deg, rgba(158,1,66,1) 10%, 
#                 rgba(213,62,79,1) 20%, rgba(244,109,67,1) 30%, 
#                 rgba(253,174,97,1) 40%, rgba(254,224,139,1) 50%, 
#                 rgba(230,245,152,1) 60%, rgba(171,221,164,1) 70%, 
#                 rgba(102,194,165,1) 80%, rgba(50,136,189,1) 90%, 
#                 rgba(94,79,162,1) 100%);
#                 display: inline-block;
#             width: 20px; height: 60px;">
#         </span>
#                 <style>
#         .multiline-span {
#             display: inline-block; /* Allows span to have block-like behavior while remaining inline */
#             width: 200px; /* Set a width to control where the text wraps */
#                 margin-left: 10px; /* Add a left indent */
#                  font-size: 12px; /* Set the text size */
#         }
#     </style>
# </head>
# <body>
#     <span class="multiline-span">
#         Mayor<br>
#                 <br>
#         Menor
#     </span>
#         """, unsafe_allow_html=True)

#def set_footer():
    #st.image("output/figures/Footer_un_cuarto.jpg", use_column_width=True)

# Función principal de Streamlit
def main():
    
    mapas()
    st.divider()
    seleccionador1, seleccionador2 = user_area_selection()
    scatters(seleccionador1, seleccionador2, column_to_plot = None)
    #column_to_plot = user_indicator_selection()
    #gauges(seleccionador1, seleccionador2, column_to_plot)
    #set_footer()

# Llamada a la función principal
if __name__ == "__main__":
    main()  
