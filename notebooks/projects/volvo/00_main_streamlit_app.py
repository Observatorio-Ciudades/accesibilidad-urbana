import streamlit as st



# Configuración de la página
st.set_page_config(page_title="Proyecto Volvo", layout="wide")

# Título de la aplicación
st.title("Comparación de Datos: Guadalajara vs Medellín")

# Descripción de la página
st.markdown("""
         Insertar la descripción del proyecto aquí.
         """)

# Selección en la barra lateral
visualizacion = st.sidebar.selectbox(
    "Selecciona la visualización:",
    ("Proyecto","Mapas", "Hallazgos")
)

if visualizacion == "Proyecto":
    import landing_page
    landing_page.display_landing_page()
elif visualizacion == "Mapas":
    import maps_page
    maps_page.display_maps_page()
elif visualizacion == "Hallazgos":
    import graphs_page
    graphs_page.display_graphs_page()
