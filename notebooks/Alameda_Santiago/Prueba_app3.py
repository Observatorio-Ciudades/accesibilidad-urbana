### Librerías ###
import geopandas as gpd
import pandas as pd
import folium
from folium.plugins import FeatureGroupSubGroup
import streamlit as st
import streamlit_folium as stf
from folium import GeoJson
import plotly.graph_objects as go
from streamlit_folium import st_folium

# Función para leer archivos geoespaciales y convertir su sistema de coordenadas a EPSG:4326
def read_file(filepath):
    return gpd.read_file(filepath).to_crs('EPSG:4326')

# Función para agregar un GeoDataFrame como capa a un mapa interactivo
def add_gdf_to_map(gdf, name, color, m):
    g = FeatureGroupSubGroup(m, name)
    m.add_child(g)
    
    # Campos que se mostrarán en el tooltip
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

# Función para agregar un GeoDataFrame como capa con marcadores a un mapa interactivo
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
    
    # Agregar marcadores en las ubicaciones del GeoDataFrame
    for _, row in gdf.iterrows():
        lat = row.geometry.y
        lon = row.geometry.x
            
        folium.Marker(
            location=[lat, lon],
            icon=folium.Icon(color=icon_color, prefix='fa', icon=icon_name)
        ).add_to(g)

# Configuración inicial de la página de Streamlit
st.set_page_config(
    page_title="Avenida Libertador Bernardo O'Higgins (Nueva Alameda), Santiago, Chile",
    page_icon=":wine_glass:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título y descripción de la página
st.title("Avenida Libertador Bernardo O'Higgins (Nueva Alameda), Santiago, Chile")
st.write("Geovisor con la información más relevante de la zona")

# Sidebar en donde se van a poner las instrucciones
with st.sidebar:
    st.write("Instrucciones de uso del geovisor")

# Variables de entrada
dir_grl = "/home/jovyan/accesibilidad-urbana/data/external"
buffer = read_file("alameda_buffer800m_gcs_v1.geojson")
hexas_santiago = read_file('santiago_hexanalysis_res8_4_5_kmh.geojson')
comunas_santi = read_file("santiago_comunasanalysis_4_5_kmh.geojson")
unidades_vecinales = read_file("santiago_unidadesvecinalesanalysis_4_5_kmh.geojson")
spyderplot = read_file('santiago_hexanalysis_res8_4_5_kmh.geojson')
santiago = spyderplot.drop(columns="geometry")
columns_santiago = santiago.columns

# Datas de prueba para la función de diferentes usuarios
bomberos = read_file(dir_grl + '/Bomberos/layer_companias_de_bomberos/layer_companias_de_bomberos_20231110080349.shp')
salud = read_file(dir_grl + '/capas_pois/salud/establec_salud_14_mayo_2021.shp')
salud = salud[salud['nom_provin'] == 'Santiago'].sample(n=100, random_state=43)
educ = read_file(dir_grl + '/capas_pois/educativo/layer_establecimientos_de_educacion_superior_20220309024111.shp')
educ = educ[educ['COD_REGION'] == 13].sample(n=100, random_state=43)

# Función para mostrar los mapas interactivos basados en el usuario seleccionado
def mapas():
    user_tabs = ["Usuario 1", "Usuario 2", "Usuario 3", "Usuario 4"]
    selected_user = st.selectbox("Seleccione el usuario", user_tabs)

    if selected_user == "Usuario 1":
        mapa_usuario_1()
    elif selected_user == "Usuario 2":
        mapa_usuario_2()
    elif selected_user == "Usuario 3":
        mapa_usuario_3()
    elif selected_user == "Usuario 4":
        mapa_usuario_4()

# Función para mostrar gráficos de dispersión polar
def scatters():
    with st.container():
        col1, col2, col3 = st.columns([0.33, 0.33, 0.33])
        with col1:
            st.write("Zona Metropolitana de Santiago")
            column_sums = santiago.sum()
            labels = columns_santiago
            sums = column_sums.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself',
                fillcolor="orchid",
                line_color='salmon'
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=False,
                        range=[0, max(sums)]
                    )),
                showlegend=False
            )
            st.plotly_chart(fig)
            st.write("Datos")
        with col2:
            st.write("Comunas de Santiago")
            selecciona_comunas = st.selectbox("Seleccione una comuna:", comunas_santi["name"].unique())
            comuna_selected = comunas_santi[comunas_santi["name"] == selecciona_comunas]
            column_sums = comuna_selected.sum()
            labels = comuna_selected.columns
            sums = column_sums.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself',
                fillcolor="orchid",
                line_color='salmon'
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=False,
                        range=[0, max(sums)]
                    )),
                showlegend=False
            )
            st.plotly_chart(fig)
            st.write("Datos")
        with col3:
            st.write("Unidades Vecinales")
            selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["name"].unique())
            unidad_selected = unidades_vecinales[unidades_vecinales["name"] == selecciona_unidades]
            column_sums_unidades = unidad_selected.sum()
            labels_unidades = unidad_selected.columns
            sums_unidades = column_sums_unidades.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself',
                fillcolor="orchid",
                line_color='salmon'
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=False,
                        range=[0, max(sums)]
                    )),
                showlegend=False
            )
            st.plotly_chart(fig)
            st.write("Datos")

# Función para determinar el nivel de un valor en la gráfica de gauge
def get_level_text(value):
    if value < 11:
        return "Muy Bajo"
    elif value < 23:
        return "Bajo"
    elif value < 35:
        return "Medio"
    elif value < 47:
        return "Alto"
    else:
        return "Muy Alto"

# Función para crear una gráfica de gauge
def create_gauge_chart(title, value):
    level_text = get_level_text(value)
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 33}},
        gauge={
            "axis": {"range": [0, 60], 'tickcolor': "black"},
            'bar': {'color': "darkslategray"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "olive",
            "steps": [
                {'range': [0, 11], 'color': "red"},
                {'range': [11, 23], 'color': "orange"},
                {'range': [23, 35], 'color': "gold"},
                {'range': [35, 47], 'color': "greenyellow"},
                {'range': [47, 60], 'color': "springgreen"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': value
            }
        }
    ))

    fig.update_layout(paper_bgcolor="whitesmoke", font={'color': "black", 'family': "Arial"})
    fig.add_annotation(
        x=0.5,
        y=0.3,
        text=level_text,
        showarrow=False,
        font=dict(size=64),
        xref="paper",
        yref="paper",
        xanchor="center",
        yanchor="middle"
    )
    return fig

# Función para mostrar las gráficas de gauge
def gauges():
    with st.container():
        col1, col2, col3, col4 = st.columns([0.25, 0.25, 0.25, 0.25])
        with col1:
            value = value
            gauge_chart = create_gauge_chart( value)
            st.plotly_chart(gauge_chart)
        with col2:
            value = value
            gauge_chart = create_gauge_chart(value)
            st.plotly_chart(gauge_chart)
        with col3:
            value = value
            gauge_chart = create_gauge_chart( value)
            st.plotly_chart(gauge_chart)
        with col4:
            value = value
            gauge_chart = create_gauge_chart(value)
            st.plotly_chart(gauge_chart)

# Función para mostrar el mapa interactivo del Usuario 1
def mapa_usuario_1():
    m = folium.Map(
        location=[buffer.geometry.centroid.y.mean(), buffer.geometry.centroid.x.mean()],
        zoom_start=14.45,
        tiles="cartodb positron"
    )

    add_gdf_to_map(buffer, "Buffer Alameda", "blue", m)
    add_gdf_to_map(hexas_santiago, "Hexágonos Análisis", "green", m)
    add_gdf_to_map(comunas_santi, "Comunas de Santiago", "red", m)

    folium.LayerControl(collapsed=False).add_to(m)

    with st.container():
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.write("Mapa Interactivo Usuario 1")
            st_folium(m, width=2000, height=700)
        with col2:
            mostrar_legenda()

# Función para mostrar el mapa interactivo del Usuario 2
def mapa_usuario_2():
    m = folium.Map(
        location=[buffer.geometry.centroid.y.mean(), buffer.geometry.centroid.x.mean()],
        zoom_start=14.45,
        tiles="cartodb positron"
    )

    add_gdf_to_map(salud, "Salud", "purple", m)
    add_gdf_to_map(hexas_santiago, "Hexágonos Análisis", "orange", m)
    add_gdf_to_map(comunas_santi, "Comunas de Santiago", "red", m)

    folium.LayerControl(collapsed=False).add_to(m)

    with st.container():
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.write("Mapa Interactivo Usuario 2")
            st_folium(m, width=2000, height=700)
        with col2:
            mostrar_legenda()

# Función para mostrar el mapa interactivo del Usuario 3
def mapa_usuario_3():
    m = folium.Map(
        location=[buffer.geometry.centroid.y.mean(), buffer.geometry.centroid.x.mean()],
        zoom_start=14.45,
        tiles="cartodb positron"
    )

    add_gdf_to_map(comunas_santi, "Comunas de Santiago", "forestgreen", m)
    add_gdf_to_map(hexas_santiago, "Hexágonos Análisis", "indianred", m)

    folium.LayerControl(collapsed=False).add_to(m)

    with st.container():
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.write("Mapa Interactivo Usuario 3")
            st_folium(m, width=2000, height=700)
        with col2:
            mostrar_legenda()

# Función para mostrar el mapa interactivo del Usuario 4
def mapa_usuario_4():
    m = folium.Map(
        location=[buffer.geometry.centroid.y.mean(), buffer.geometry.centroid.x.mean()],
        zoom_start=14.45,
        tiles="cartodb positron"
    )

    add_gdf_to_map(unidades_vecinales, "Unidades Vecinales", "maroon", m)
    add_gdf_to_map(hexas_santiago, "Hexágonos Análisis", "navy", m)

    folium.LayerControl(collapsed=False).add_to(m)

    with st.container():
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.write("Mapa Interactivo Usuario 4")
            st_folium(m, width=2000, height=700)
        with col2:
            mostrar_legenda()

# Función para mostrar la leyenda del mapa
def mostrar_legenda():
    st.markdown('### Legend')
    st.markdown('#### Elements')
    st.markdown(f""" 
        <span style="display: inline-flex; align-items: center;"> 
            <span style="background-color: rgba(53, 202, 12, 0.6); border: 1px solid rgba(53, 202, 12, 0.6); display: inline-block;
            width: 20px; height: 20px;"></span>
            <span style="padding-left: 5px;">Área Metropolitana de Santiago</span>
        </span>
        """, unsafe_allow_html=True)
    st.markdown(f"""
        <span style="display: inline-flex; align-items: center;">
            <span style="background-color: rgba(0, 99, 194, 0.79); border: 1px solid rgba(0, 99, 194, 0.79); display: inline-block;
            width: 20px; height: 20px;"></span>
            <span style="padding-left: 5px;">Buffer Avenida Nueva Alameda</span>
        </span>
        """, unsafe_allow_html=True)
    st.markdown(f"""
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
        <span style="display: inline-flex; align-items: center;">
            <i class="fas fa-fire-extinguisher" style="color: red; font-size: 20px;"></i>
            <span style="padding-left: 5px;">Compañias de Bomberos</span>
        </span>
        """, unsafe_allow_html=True)

# Función principal de Streamlit
def main():
    mapas()
    scatters()
    gauges()

# Llamada a la función principal
main()
