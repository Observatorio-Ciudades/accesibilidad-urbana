
#Código GeoVisor en Streamlit ../../data/external/buffernuevaalameda/buffer 800m nueva alameda.shp
import geopandas as gpd
import matplotlib.pyplot as plt
import os 
import sys
module_path = os.path.abspath(os.path.join('../../'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
import pandas as pd
import numpy as np
import leafmap
import ipywidgets as widgets
from ipywidgets import interact, Checkbox
import folium
from folium import GeoJson
from folium.plugins import FeatureGroupSubGroup
import streamlit as st
import streamlit_folium as stf


title = 'Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile'
dir = f'data/external/'

buffer = gpd.read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp').to_crs('EPSG:4326')
bomberos = gpd.read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp').to_crs('EPSG:4326')
educ = gpd.read_file(dir + 'Establecimientos educacionales/establecimientos_educacionales_2021.shp').to_crs('EPSG:4326')
salud = gpd.read_file(dir + 'Establecimientos salud/establec_salud_14_mayo_2021.shp').to_crs('EPSG:4326')
poly_santiago = gpd.read_file(dir + 'Poligono Santiago/PoligonoSantiago.shp').to_crs('EPSG:4326')           
m = folium.Map(location=[buffer['geometry'].centroid.y.mean(), buffer['geometry'].centroid.x.mean()], zoom_start=14.45)
def main():
    st.set_page_config(
        page_title= 'Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile',
        page_icon=":wine_glass:",
        layout = "wide",
        initial_sidebar_state="expanded")
    with open('style.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    with st.sidebar:

        st.header('Información')
        st.subheader('Funcionalidades')


    st.title('Avenida Libertador Bernardo O\'Higgins (Nueva Alameda), Santiago, Chile')
    st.write("Geovisor con la información más relevante de la zona")

    tab1, tab2 = st.tabs(["Mapa Geo Visor", "Estadísticas"])

    with tab2:
        st.title('Estadísticas importantes')
        st.write("""
        Aquí se podrían generar gráficos con la información pertinente si es que así se desea
        \n Tal vez poner información del tráfico
        """)

    with tab1:
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
        add_gdf_marker(educ, "Establecimientos educacionales", "pink", 'graduation-cap', 'pink')
        add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')
        
        # Agregar control de capas
        folium.LayerControl(collapsed=False).add_to(m)

        
        st.title("Geovisor Eje Nueva Alameda con servicios")
        st_data = st_folium(m, width=700, height=500)

