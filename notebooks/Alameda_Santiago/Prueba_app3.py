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


# Función para leer los archivos gpd y cambiarles el formato de coordenada
def read_file(filepath):
    return gpd.read_file(filepath).to_crs('EPSG:4326') 

# Función para agregar un geodataframe como capa al mapa interactivo
def add_gdf_to_map(gdf, name, color, m):
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
columns_santiago =  santiago.select_dtypes(include='number').columns

# Datas de prueba para la función de diferentes usuarios
bomberos = read_file(dir_grl + '/Bomberos/layer_companias_de_bomberos/layer_companias_de_bomberos_20231110080349.shp')
salud = read_file(dir_grl + '/capas_pois/salud/establec_salud_14_mayo_2021.shp')
salud = salud[salud['nom_provin'] == 'Santiago'].sample(n=100, random_state=43)
educ = read_file(dir_grl + '/capas_pois/educativo/layer_establecimientos_de_educacion_superior_20220309024111.shp')
educ = educ[educ['COD_REGION'] == 13].sample(n=100, random_state=43)

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

def scatters():
    with st.container():
        col1, col2, col3 = st.columns([0.33, 0.33, 0.33])
        with col1:
            st.write("Zona Metropolitana de Santiago")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            column_sums = santiago[["enjoying", "living", "learning", "working", "supplying", "caring"]].sum()
            labels = column_sums.index
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
                        visible=True,
                        range=[0, max(sums)]
                    )),
                showlegend=False
            )
            st.plotly_chart(fig)
            st.write("Datos")
        with col2:
            st.write("Comunas de Santiago")
            selecciona_comunas = st.selectbox("Seleccione una comuna:", comunas_santi["hqsl"].unique())
            comuna_selected = comunas_santi[comunas_santi["hqsl"] == selecciona_comunas]
            column_sums = comuna_selected[["enjoying", "living", "learning", "working", "supplying", "caring"]].sum()
            labels = column_sums.index
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
                        visible=True,
                        range=[0, max(sums)]
                    )),
                showlegend=False
            )
            st.plotly_chart(fig)
            st.write("Datos")
        with col3:
            st.write("Unidades Vecinales")
            selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["COD_UNICO_"].unique())
            unidad_selected = unidades_vecinales[unidades_vecinales["COD_UNICO_"] == selecciona_unidades]
            column_sums_unidades = unidad_selected[["enjoying", "living", "learning", "working", "supplying", "caring"]].sum()
            labels_unidades = column_sums_unidades.index
            sums_unidades = column_sums_unidades.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums_unidades,
                theta=labels_unidades,
                fill='toself',
                fillcolor="orchid",
                line_color='salmon'
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, max(sums_unidades)]
                    )),
                showlegend=False
            )
            st.plotly_chart(fig)
            st.write("Datos")
            

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

def create_gauge_chart(title, value):
    level_text = get_level_text(value)
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 26}},
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
        font=dict(size= 34),
        xref="paper",
        yref="paper",
        xanchor="center",
        yanchor="middle"
    )
    return fig

# Cargar los datos
data_select = read_file("santiago_alamedaanalysis_4_5_kmh.geojson")
alameda_data = data_select.loc[data_select['name'] == "alameda"].iloc[0]

def gauges():
    with st.container():
                
        col1, col2, col3 = st.columns([0.33, 0.33, 0.33])
        
        with col1:
            # Crear gráfico para Alameda
            column_to_plot = st.selectbox(
            "Seleccione la característica a analizar",
            ["Sociability", "Wellbeing", "Environmental_Impact"]
        )
            value = alameda_data[column_to_plot.lower()]
            gauge_chart = create_gauge_chart(column_to_plot, value)
            st.plotly_chart(gauge_chart)
            
        with col2:
            # Seleccionar comuna
            selecciona_comunas = st.selectbox("Seleccione una comuna", comunas_santi["hqsl"].unique(), key='comuna_gauge')
            comuna_selected = comunas_santi[comunas_santi["hqsl"] == selecciona_comunas]
            value = comuna_selected[column_to_plot.lower()].sum()
            gauge_chart = create_gauge_chart(column_to_plot, value)
            st.plotly_chart(gauge_chart)
            
        with col3:
            # Seleccionar unidad vecinal
            selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["COD_UNICO_"].unique(), key='unidad_gauge')
            unidad_selected = unidades_vecinales[unidades_vecinales["COD_UNICO_"] == selecciona_unidades]
            value = unidad_selected[column_to_plot.lower()].sum()
            gauge_chart = create_gauge_chart(column_to_plot, value)
            st.plotly_chart(gauge_chart)

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
