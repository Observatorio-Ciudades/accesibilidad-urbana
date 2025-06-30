import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd


def display_maps_page():
    # Configuración de la página
    st.set_page_config(page_title="Proyecto Volvo", layout="wide")
    # Para mapas: selección individual de ciudad
    ciudad_seleccionada = st.selectbox(
        "Seleccione la ciudad que desea visualizar:",
        ('Guadalajara', 'Medellín'),
        key='ciudad_mapas'
    )
    mapas()


if "map" not in st.session_state:
    st.session_state.map = None
    st.session_state.legend = None

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

    with st.container():
        st.markdown('<div class="fixed-height-container"><div class="fixed-height-content">',
                    unsafe_allow_html=True)
        col1, col2 = st.columns([0.88, 0.12])
        if st.session_state.map is None:
            with col1:
                m = create_map()
                st.session_state.map = m
                st.components.v1.html(m.render(), height=750)

            with col2:
                legend = mostrar_legenda()
                st.session_state.legend = legend
        else:
            with col1:
                st.components.v1.html(st.session_state.map.render(), height=750) #Optimizar el renderizado.
            with col2:
                mostrar_legenda()

        st.markdown('</div></div>', unsafe_allow_html=True)


def load_geojson_files():
    grl_dir = '../../../data/processed/vref/'
    gdf_phyisical_var = gpd.read_file(grl_dir+'edges_physicalvariablesv3_poligonosestudio.gpkg')
    gdf_proximity = gpd.read_file(grl_dir+'volvo_wgtproxanalysis_2024_mza_hex9.geojson')
    gdf_polygons = gpd.read_file(grl_dir+'PolígonosEstudio.gpkg')
    return gdf_phyisical_var, gdf_proximity, gdf_polygons

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



def create_map():
    gdf_phyisical_var, gdf_proximity, gdf_polygons = load_geojson_files()
    # f = folium.Figure(width=1800, height=500)
    m = folium.Map(
        location=[gdf_proximity.geometry.centroid.y.mean(),
        gdf_proximity.geometry.centroid.x.mean()],
        zoom_start=12,
        tiles="cartodb positron",
    )

    # Add HQSL choropleth to map
    prox_map=folium.FeatureGroup(name='Proximity analysis', show=True)
    m.add_child(prox_map)
    folium.Choropleth(
            geo_data=gdf_proximity,
            name="Proximity analysis",
            data=gdf_proximity,
            key_on="feature.properties.hex_id",
            columns=["hex_id", "wgt_15_min_v3"],
            fill_color="RdPu",
            fill_opacity=0.5,
            line_opacity=0.05,
            legend_name="Proximity analysis",
        ).geojson.add_to(prox_map)

    # Add calidad de espacio publico choropleth to map
    physical_var_map=folium.FeatureGroup(name='Physical variables', show=False)
    m.add_child(physical_var_map)
    folium.Choropleth(
            geo_data=gdf_phyisical_var,
            name="Physical variables",
            data=gdf_phyisical_var,
            key_on="feature.properties.fid_txt",
            columns=["fid_txt", "ndvi_mean"],
            fill_color="Spectral",
            fill_opacity=0.75,
            line_opacity=0,
            legend_name="Physical variables",
        ).geojson.add_to(physical_var_map)




    # add_gdf_to_map(hexas_santiago, "Hexágonos Análisis", "green", m)

    # add_gdf_to_map(uv_geom, "Unidades Vecinales", "red", m,
    #                weight=1, fill_opacity=0, show=False)

    # add_gdf_to_map(comunas_geom, "Comunas", "white", m,
    #                weight=2, fill_opacity=0, show=True)

    # add_gdf_to_map(buffer, "Nueva Alameda", "white", m,
    #                     fill_opacity=0.5, weight=0.5, show=True)

    # st.write("Mapa Interactivo Usuario 1")
    # avoid page reload
    folium.LayerControl(collapsed=False, position="topleft",).add_to(m)
    # st_folium(m, width=1800, height=250, returned_objects=[]) # height 700
    # Test for static map
    # folium_static(m, width=1000, height=500) # height 700
    # Test remove returned_objects
    # st_map = st_folium(m, width=1800, height=500, returned_objects=[]) # height 700

    # Working section
    # st.components.v1.html(folium.Figure().add_child(m).render(), height=500)


    # st.html("<style> .main {overflow: hidden} </style>")
    #css = '''
    #<style>
    #section.main > div:has(~ footer ) {
    #    padding-bottom: 5px;
    #}
    #</style>
    #'''
    #st.markdown(css, unsafe_allow_html=True)''
    return folium.Figure().add_child(m)


def mostrar_legenda():

    # Add whitebox background
    # Define the HTML and CSS for the white box
    # Define the HTML and CSS for the background white box

    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')
    st.markdown(' ')

    st.markdown(
    """
    <style>
        .centered-text {
            text-align: center; /* Center the text horizontally */
            font-size: 16px; /* Set the text size */
            width: 20%; /* Ensure the text spans the entire container width */
            font-weight: bold; /* Make the text bold */
        }
    </style>

    <div class="centered-text">
        Simbología
    </div>
    """,
    unsafe_allow_html=True)

    # st.markdown('#### Elements')
    st.markdown(f"""
        <span style="display: inline-flex; align-items: center;">
            <span style="background-color: rgba(227, 227, 227, 0.5); border: 1px solid rgba(0, 0, 0, 1); display: inline-block;
            width: 20px; height: 20px;"></span>
            <span style="padding-left: 5px; font-size: 12px;">Nueva Alameda</span>
        </span>
        """, unsafe_allow_html=True)
    st.markdown(f"""
        <span style="display: inline-flex; align-items: center;">
            <span style="background-color: rgba(53, 202, 12, 0); border: 1px solid rgba(0, 0, 0, 1); display: inline-block;
            width: 20px; height: 20px;"></span>
            <span style="padding-left: 5px; font-size: 12px;">Unidades Vecinales</span>
        </span>
        """, unsafe_allow_html=True)
    st.markdown(f"""
        <span style="display: inline-flex; align-items: center;">
            <span style="background-color: rgba(0, 99, 194, 0); border: 3px solid rgba(0, 0, 0, 1); display: inline-block;
            width: 20px; height: 20px;"></span>
            <span style="padding-left: 5px; font-size: 12px;">Comunas</span>
        </span>
        """, unsafe_allow_html=True)

    # HQSL Choropleth legend

    st.markdown(
    """
    <style>
        .centered-text {
            text-align: center; /* Center the text horizontally */
            font-size: 12px; /* Set the text size */
            width: 20%; /* Ensure the text spans the entire container width */
            font-weight: bold; /* Make the text bold */
        }
    </style>

    <div class="centered-text">
        Calidad de Vida Social (HQSL)
    </div>
    """,
    unsafe_allow_html=True
)
    st.markdown("""
        <span style="display: inline-flex; align-items: center;">
            <span style=" background: rgb(63,94,251);
                background: linear-gradient(0deg, rgba(254,235,226,1) 17%,
                rgba(252,197,192,1) 34%, rgba(250,159,181,1) 51%,
                rgba(247,104,161,1) 68%,
                rgba(197,27,138,1) 85%, rgba(122,1,119,1) 100%);
                display: inline-block;
            width: 20px; height: 60px;">
        </span>
                <style>
        .multiline-span {
            display: inline-block; /* Allows span to have block-like behavior while remaining inline */
            width: 200px; /* Set a width to control where the text wraps */
                margin-left: 10px; /* Add a left indent */
                 font-size: 12px; /* Set the text size */
        }
    </style>
</head>
<body>
    <span class="multiline-span">
        Mayor<br>
                <br>
        Menor
    </span>
        """, unsafe_allow_html=True)

    # Calidad del espacio público legend

    st.markdown(
    """
    <style>
        .centered-text {
            text-align: left; /* Center the text horizontally */
            font-size: 12px; /* Set the text size */
            width: 100%; /* Ensure the text spans the entire container width */
            font-weight: bold; /* Make the text bold */
            margin-bottom: 0; /* Remove spacing below the text */
        }
    </style>

    <div class="centered-text">
        Calidad del espacio público para la movilidad activa
    </div>
    """,
    unsafe_allow_html=True
)
    st.markdown("""
        <span style="display: inline-flex; align-items: center;">
            <span style=" background: rgb(158,1,66);
            background: linear-gradient(0deg, rgba(158,1,66,1) 10%,
                rgba(213,62,79,1) 20%, rgba(244,109,67,1) 30%,
                rgba(253,174,97,1) 40%, rgba(254,224,139,1) 50%,
                rgba(230,245,152,1) 60%, rgba(171,221,164,1) 70%,
                rgba(102,194,165,1) 80%, rgba(50,136,189,1) 90%,
                rgba(94,79,162,1) 100%);
                display: inline-block;
            width: 20px; height: 60px;">
        </span>
                <style>
        .multiline-span {
            display: inline-block; /* Allows span to have block-like behavior while remaining inline */
            width: 200px; /* Set a width to control where the text wraps */
                margin-left: 10px; /* Add a left indent */
                 font-size: 12px; /* Set the text size */
        }
    </style>
</head>
<body>
    <span class="multiline-span">
        Mayor<br>
                <br>
        Menor
    </span>
        """, unsafe_allow_html=True)
