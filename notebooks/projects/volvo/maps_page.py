import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
from folium.plugins import FeatureGroupSubGroup
from folium import GeoJson


def display_maps_page():
    # Configuración de la página
    st.set_page_config(page_title="Proximity vs Walkability", layout="wide")
    # Para mapas: selección individual de ciudad
    selected_city = st.selectbox(
        "Seleccione la ciudad que desea visualizar:",
        ('Guadalajara', 'Medellín'),
        key='ciudad_mapas'
    )
    mapas(selected_city)


if "map" not in st.session_state:
    st.session_state.map = None
    st.session_state.legend = None
    st.session_state.selected_city = None

def mapas(city):
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
        if st.session_state.map is None and st.session_state.selected_city is None:
            with col1:
                m = create_map()
                st.session_state.map = m
                st.components.v1.html(m.render(), height=750)
                st.session_state.selected_city = city

            with col2:
                legend = create_legend()
                st.session_state.legend = legend
        else:
            if st.session_state.map is not None and st.session_state.selected_city == city:
                with col1:
                    st.components.v1.html(st.session_state.map.render(), height=750) #Optimizar el renderizado.
                with col2:
                    create_legend()
            else:
                with col1:
                    m = create_map()
                    st.session_state.map = m
                    st.components.v1.html(m.render(), height=750)
                    st.session_state.selected_city = city
                with col2:
                    create_legend()


        st.markdown('</div></div>', unsafe_allow_html=True)

def create_map():
    (gdf_phyisical_var, gdf_proximity,
        gdf_polygons, gdf_walkability_index)= load_geojson_files()
    # f = folium.Figure(width=1800, height=500)
    m = folium.Map(
        location=[gdf_proximity.geometry.centroid.y.mean(),
        gdf_proximity.geometry.centroid.x.mean()],
        zoom_start=12,
        tiles="cartodb positron",
    )

    # Create custom panes with specific z-index values
    m.get_root().html.add_child(folium.Element("""
    <style>
    .leaflet-bottom-pane { z-index: 100; }
    .leaflet-middle-pane { z-index: 200; }
    .leaflet-top-pane { z-index: 300; }
    </style>
    """))

    # Add polygons to map
    add_gdf_to_map(gdf_polygons, "Study area", "green", m,
        weight=3, dashArray="5,5", show=True,
        pane="top-pane")

    add_walkability_index_map(m, gdf_walkability_index)

    add_proximity_map(m, gdf_proximity)

    physical_dict = {
        'Population density':'cat_density',
        'Vegetation index':'cat_vegetation',
        'Slope':'cat_slope',
        'Intersection density':'cat_intersection',
        'Sidewalk':'cat_sidewalk',
        'Land use diversity':'cat_land_use'
    }
    for key, value in physical_dict.items():
            add_physical_variables_map(m,
                gdf_phyisical_var,
             value,
            key,
           )

    folium.LayerControl(collapsed=False, autoZIndex=True,
        position="topleft",).add_to(m)


    return folium.Figure().add_child(m)

def add_proximity_map(m, gdf_proximity):
    # Add proximity choropleth to map
    prox_map=folium.FeatureGroup(name='Proximity analysis', show=False)
    m.add_child(prox_map)
    folium.Choropleth(
            geo_data=gdf_proximity,
            name="Proximity analysis",
            data=gdf_proximity,
            key_on="feature.properties.fid_txt",
            columns=["fid_txt", "wgt_proximity_15min"],
            fill_color="RdPu",
            fill_opacity=0.5,
            line_opacity=0.05,
            legend_name="Proximity analysis",
            pane = 'middle-pane',
        ).geojson.add_to(prox_map)

def add_walkability_index_map(m, gdf_walkability_index):
    # Add proximity choropleth to map
    walkability_map=folium.FeatureGroup(name='Walkability Index', show=True)
    m.add_child(walkability_map)
    folium.Choropleth(
            geo_data=gdf_walkability_index,
            name="Walkability Index",
            data=gdf_walkability_index,
            key_on="feature.properties.fid_txt",
            columns=["fid_txt", "WI_int"],
            fill_color="Spectral",
            fill_opacity=0.5,
            line_opacity=0.05,
            legend_name="Walkability Index",
            pane = 'middle-pane',
        ).geojson.add_to(walkability_map)

def add_physical_variables_map(m, gdf_phyisical_var, physical_var, phyisical_var_name, parent_group=None):
    # Add physical variables choropleth to map
    physical_var_map = folium.FeatureGroup(name=f'Physical Variables - {phyisical_var_name}', show=False)
    m.add_child(physical_var_map)

    folium.Choropleth(
            geo_data=gdf_phyisical_var,
            name=f'Physical Variables - {phyisical_var_name}',
            data=gdf_phyisical_var,
            key_on="feature.properties.fid_txt",
            columns=["fid_txt", physical_var],
            fill_color="viridis",
            fill_opacity=0.75,
            line_opacity=0,
            legend_name="Physical variables",
            pane = 'middle-pane',
        ).geojson.add_to(physical_var_map)


def load_geojson_files():
    grl_dir = '../../../data/processed/vref/'
    gdf_phyisical_var = gpd.read_file(grl_dir+'/tmp/bufferedges_physicalvar_poligonosestudio.gpkg')
    gdf_proximity = gpd.read_file(grl_dir+'/tmp/volvo_wgtproximityanalysis_poligonosestudio.gpkg')
    gdf_polygons = gpd.read_file(grl_dir+'PolígonosEstudio.gpkg')
    gdf_walkability_index = gpd.read_file(grl_dir+'/tmp/bufferedges_diss_walkabilityindex_poligonosetudio.gpkg')
    return gdf_phyisical_var, gdf_proximity, gdf_polygons, gdf_walkability_index

# Función para agregar un geodataframe como capa al mapa interactivo
def add_gdf_to_map(gdf, name, color, m, weight=1,
                   fill_opacity=0, dashArray=None, show=False,
                   pane="middle-pane"):
    # g = FeatureGroupSubGroup(m, name, show=show)
    g = folium.FeatureGroup(name=name, show=show)
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
        tooltip=folium.GeoJsonTooltip(fields=fields, labels=True, sticky=True),
        # pane=pane,
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

def create_legend():

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
    Legend
    </div>
    """,
    unsafe_allow_html=True)

    # st.markdown('#### Elements')

    st.markdown(f"""
        <span style="display: inline-flex; align-items: center;">
            <span style="background-color: rgba(0, 99, 194, 0); border: 3px dashed rgba(0, 0, 0, 1); display: inline-block;
            width: 20px; height: 20px;"></span>
            <span style="padding-left: 5px; font-size: 12px;">Study area</span>
        </span>
        """, unsafe_allow_html=True)

    # Proximity analysis legend

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
        Proximity analysis
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
        Higher<br>
                <br>
                Lower
    </span>
        """, unsafe_allow_html=True)

    # Walkability index legend
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
        Walkability Index
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
        Higher<br>
                <br>
                Lower
    </span>
        """, unsafe_allow_html=True)

    # Physical variables legend
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
    Physical Variables
    </div>
    """,
    unsafe_allow_html=True
    )
    st.markdown("""
        <span style="display: inline-flex; align-items: center;">
            <span style=" background: rgb(158,1,66);
            background: linear-gradient(0deg,
                rgba(68,1,84,1) 10%,
                rgba(72,40,120,1) 20%,
                rgba(62,74,137,1) 30%,
                rgba(49,104,142,1) 40%,
                rgba(38,130,142,1) 50%,
                rgba(31,158,137,1) 60%,
                rgba(53,183,121,1) 70%,
                rgba(109,205,89,1) 80%,
                rgba(180,222,44,1) 90%,
                rgba(253,231,37,1) 100%);
            display: inline-block;
            width: 20px;
            height: 60px;">
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
        Higher<br>
                <br>
                Lower
    </span>
        """, unsafe_allow_html=True)
