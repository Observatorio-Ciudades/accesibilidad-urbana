import streamlit as st


def display_maps_page():
    # Configuración de la página
    st.set_page_config(page_title="Proyecto Volvo", layout="wide")
    # Para mapas: selección individual de ciudad
    ciudad_seleccionada = st.selectbox(
        "Seleccione la ciudad que desea visualizar:",
        ('Guadalajara', 'Medellín'),
        key='ciudad_mapas'
    )

# Función para cargar polígonos de estudio
#     def cargar_poligonos_estudio(ruta_archivo):
#         """
#         Carga el archivo PoligonosEstudio.gpkg y lo añade al mapa
#         """
#         try:
#             if os.path.exists(ruta_archivo):
#                 gdf_poligonos = gpd.read_file(ruta_archivo)
#                 if 'geometry' not in gdf_poligonos.columns or gdf_poligonos.empty:
#                     return None
#                 return gdf_poligonos
#             else:
#                 return None
#         except Exception as e:
#             return None
