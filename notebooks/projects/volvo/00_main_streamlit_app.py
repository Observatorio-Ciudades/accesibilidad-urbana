import streamlit as st

# Configuración de la página - debe ser lo primero
st.set_page_config(page_title="Proyecto Volvo", layout="wide")

# Importaciones locales después de set_page_config
import landing_page
import maps_page
import graphs_page

# Título de la aplicación
st.title("Comparación de Datos: Guadalajara vs Medellín")

# Descripción de la página
st.markdown("""
    Insertar la descripción del proyecto aquí.
""")

# Selección en la barra lateral
visualizacion = st.sidebar.selectbox(
    "Selecciona la visualización:",
    ("Proyecto", "Mapas", "Hallazgos")
)

if visualizacion == "Proyecto":
    landing_page.display_landing_page()
elif visualizacion == "Mapas":
    maps_page.display_maps_page()
elif visualizacion == "Hallazgos":
    graphs_page.display_graphs_page()
