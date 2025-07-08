import streamlit as st

# Configuración de la página - debe ser lo primero
st.set_page_config(page_title="Proximity vs Walkability", layout="wide")

# Importaciones locales después de set_page_config
import landing_page
import maps_page
import graphs_page

# Título de la aplicación
st.title("From accesibility to walkability 🚶‍♂️🌎")

# Descripción de la página
# st.markdown("""
#     Insertar la descripción del proyecto aquí.
#   """)

# Selección en la barra lateral
visualizacion = st.sidebar.selectbox(
    "Selecciona la visualización:",
    ("Project overlook", "Cartography", "Findings")
)

if visualizacion == "Project overlook":
    landing_page.display_landing_page()
elif visualizacion == "Cartography":
    maps_page.display_maps_page()
elif visualizacion == "Findings":
    graphs_page.display_graphs_page()
