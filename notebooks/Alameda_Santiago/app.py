
#Código GeoVisor 
import geopandas as gpd
import folium
from folium.plugins import FeatureGroupSubGroup
import streamlit as st
import streamlit_folium as stf
from folium import GeoJson

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

# Tabs
tab1, tab2 = st.tabs(["Mapa Geo Visor", "Estadísticas"])

with tab2:
    st.title('Estadísticas importantes')
    st.write("""
    Aquí se podrían generar gráficos con la información pertinente si es que así se desea.
    \n Tal vez poner información del tráfico.
    """)

with tab1:
    # Función para leer archivos
    def read_file(filepath):
        return gpd.read_file(filepath).to_crs('EPSG:4326')

    # Leer los archivos necesarios
    dir = 'data/external/'
    buffer = read_file(dir+'buffernuevaalameda/buffer 800m nueva alameda.shp')
    bomberos = read_file(dir + 'Companias de bomberos/layer_companias_de_bomberos_20231110080349.shp')
    educ = read_file(dir + 'Establecimientos educacionales/establecimientos_educacionales_2021.shp')
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
    add_gdf_marker(bomberos, "Compañías de Bomberos", "red", 'fire-extinguisher', 'red')
    add_gdf_marker(educ, "Establecimientos educacionales", "pink", 'graduation-cap', 'pink')
    add_gdf_marker(salud, "Establecimientos de salud", "orange", 'heartbeat', 'orange')
        
        # Agregar control de capas
    folium.LayerControl(collapsed=False).add_to(m)

        
    st.title("Geovisor Eje Nueva Alameda con servicios")
    st_data = stf.st_folium(m, width=700, height=500)

