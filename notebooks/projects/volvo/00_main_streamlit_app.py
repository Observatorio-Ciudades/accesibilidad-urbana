import streamlit as st



# Configuración de la página
st.set_page_config(page_title="Proximity vs Walkability", layout="wide")

# Título de la aplicación
st.title("From accesibility to walkability 🚶‍♂️🌎")

# Descripción de la página
# st.markdown("""
#         Insertar la descripción del proyecto aquí.
#          """)

# Selección en la barra lateral
visualizacion = st.sidebar.selectbox(
    "Select the webpage:",
    ("Project overlook","Cartography", "Findings")
)

if visualizacion == "Project overlook":
    import landing_page
    landing_page.display_landing_page()
elif visualizacion == "Cartography":
    import maps_page
    # import map_page
    # map_page.main()
    maps_page.display_maps_page()
elif visualizacion == "Findings":
    import graphs_page
    graphs_page.display_graphs_page()
