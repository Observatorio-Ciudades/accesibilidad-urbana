## Librerías ###
import streamlit as st
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