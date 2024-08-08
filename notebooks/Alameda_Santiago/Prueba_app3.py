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

# Estilos para ocultar el botón de pantalla completa
hide_img_fs = '''
            <style>
            button[title="View fullscreen"]{
                visibility: hidden;}
            </style>
            '''

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
    page_title="Diagnóstico Santiago",
    page_icon=":hexagon:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título y descripción de la página
st.title("¿Cómo vive la gente en Santiago?")
st.write("Geovisor con la información más relevante de la zona")

# Sidebar en donde se van a poner las instrucciones
with st.sidebar:
    st.write("Instrucciones de uso del geovisor")

# Variables de entrada
dir_grl = "../../data/external/santiago/"
buffer = read_file(dir_grl+"alameda_all_buffer800m_gcs_v1.geojson")
hexas_santiago = read_file(dir_grl+'santiago_hexanalysis_res8_4_5_kmh.geojson')
comunas_santi = read_file(dir_grl+"santiago_comunasanalysis_4_5_kmh.geojson")
unidades_vecinales = read_file(dir_grl+"santiago_unidadesvecinalesanalysis_4_5_kmh.geojson")
alameda = read_file(dir_grl+'santiago_alamedaanalysis_4_5_kmh.geojson')
# santiago = spyderplot.drop(columns="geometry")
columns_santiago =  alameda.select_dtypes(include='number').columns

# Datas de prueba para la función de diferentes usuarios
bomberos = read_file(dir_grl + '/Bomberos/layer_companias_de_bomberos_20231110080349.shp')
salud = read_file(dir_grl + '/salud/establec_salud_14_mayo_2021.shp')
salud = salud[salud['nom_provin'] == 'Santiago'].sample(n=100, random_state=43)
educ = read_file(dir_grl + '/educativo/layer_establecimientos_de_educacion_superior_20220309024111.shp')
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

def user_area_selection():
    st.markdown("## Selecciona tu área de interés")
    st.markdown("#### Indicador de Alta Calidad de Vida Social")
    col1, col2, col3 = st.columns([0.30, 0.30, 0.30])
    with col1:
        st.write("Nueva Alameda")
    with col2:
        st.write("Comunas")    
        selecciona_comunas = st.selectbox("Seleccione una comuna:", comunas_santi["Comuna"].unique())
    with col3:
        st.write("Unidades Vecinales")    
        seleeciona_unidad_vecinal = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["COD_UNICO_"].unique())
    return selecciona_comunas, seleeciona_unidad_vecinal

def user_indicator_selection():
    st.write("Indicadores de bienestar")
    col1, _, _ = st.columns([0.30, 0.30, 0.30])
    with col1:
        column_to_plot = st.selectbox(
        "Seleccione el indicador de bienestar",
        ["Sociability", "Wellbeing", "Environmental_Impact"])
    return column_to_plot

def scatters(selecciona_comunas, selecciona_unidades):
    with st.container():
        col1, col2, col3 = st.columns([0.30, 0.30, 0.30])
        with col1:
            # st.write("Nueva Alameda")
            # st.write("")
            # st.write("")
            # st.write("")
            # st.write("")
            # st.write("")
            column_sums = alameda[["enjoying", "living", "learning", "working", "supplying", "caring"]].sum()
            labels = column_sums.index
            sums = column_sums.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself',
                fillcolor="rgba(28,28,255,0.3)",
                line_color="rgba(28,28,255,0.2)",
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 10]
                    )),
                showlegend=False,
            )

            
            st.markdown(hide_img_fs, unsafe_allow_html=True)

            st.plotly_chart(fig, use_container_width=True, 
                            use_container_height=True,
                            config = {'displayModeBar': True}
                            )
            # st.write("Datos")
        
        with col2:
            # st.write("Comunas de Santiago")
            # selecciona_comunas = st.selectbox("Seleccione una comuna:", comunas_santi["Comuna"].unique())
            comuna_selected = comunas_santi[comunas_santi["Comuna"] == selecciona_comunas]
            column_sums = comuna_selected[["enjoying", "living", "learning", "working", "supplying", "caring"]].sum()
            labels = column_sums.index
            sums = column_sums.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums,
                theta=labels,
                fill='toself',
                fillcolor="rgba(28,28,255,0.3)",
                line_color="rgba(28,28,255,0.2)",
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 10]
                    )),
                showlegend=False,
            )

            
            st.markdown(hide_img_fs, unsafe_allow_html=True)

            st.plotly_chart(fig, use_container_width=True, 
                            use_container_height=True,
                            config = {'displayModeBar': True})
            # st.write("Datos")
        
        with col3:
            # st.write("Unidades Vecinales")
            # selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["COD_UNICO_"].unique())
            unidad_selected = unidades_vecinales[unidades_vecinales["COD_UNICO_"] == selecciona_unidades]
            column_sums_unidades = unidad_selected[["enjoying", "living", "learning", "working", "supplying", "caring"]].sum()
            labels_unidades = column_sums_unidades.index
            sums_unidades = column_sums_unidades.values

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=sums_unidades,
                theta=labels_unidades,
                fill='toself',
                fillcolor="rgba(28,28,255,0.3)",
                line_color="rgba(28,28,255,0.2)",
            ))

            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 10]
                    )),
                showlegend=False,
            )

            
            st.markdown(hide_img_fs, unsafe_allow_html=True)

            st.plotly_chart(fig, use_container_width=True, 
                            use_container_height=True,
                            config = {'displayModeBar': True})
            # st.write("Datos")
            

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

    layout = {
    'xaxis': {
        'showticklabels': False,
        'showgrid': False,
        'zeroline': False,
    },
    'yaxis': {
        'showticklabels': False,
        'showgrid': False,
        'zeroline': False,
    },
    'shapes': [
        {
            'type': 'path',
            'path': 'M 0.235 0.5 L 0.24 0.65 L 0.245 0.5 Z',
            'fillcolor': 'rgba(44, 160, 101, 0.5)',
            'line': {
                'width': 0.5
            },
            'xref': 'paper',
            'yref': 'paper'
        }
    ],
    'annotations': [
        {
            'xref': 'paper',
            'yref': 'paper',
            'x': 0.23,
            'y': 0.45,
            'text': '50',
            'showarrow': False
        }
    ]
    }


    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 26}},
        gauge={
            #"axis": {"range": [0, 60], 'tickcolor': "black"}.
            'axis': {'range': [0, 60], 'tickvals': [6,18,30,42,54], 'ticktext': ['Muy bajo', 'Bajo', 'Medio', 'Alto', 'Muy alto']},
            'bar': {'color': "rgba(28,28,255,0.0)"},
            'bgcolor': "white",
            'borderwidth': 0.5,
            'bordercolor': "white",
            "steps": [
                {'range': [0, 12], 'color': "#aaaaff"},
                {'range': [12, 24], 'color': "#8e8eff"},
                {'range': [24, 36], 'color': "#7171ff"},
                {'range': [36, 48], 'color': "#5555ff"},
                {'range': [48, 60], 'color': "#1c1cff"}
            ],
            'threshold': {
                'line': {'color': "white", 'width':10},
                'thickness': 1,
                'value': value
            }
        }
    ))

    fig.update_layout(#paper_bgcolor="whitesmoke", 
        font={'color': "black", 'family': "Arial"},
                      yaxis_visible=False, yaxis_showticklabels=False,
                      )
    
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
data_select = read_file(dir_grl+"santiago_alamedaanalysis_4_5_kmh.geojson")
alameda_data = data_select.loc[data_select['name'] == "alameda"].iloc[0]

def gauges(selecciona_comunas, selecciona_unidades, column_to_plot):
    with st.container():
                
        col1, col2, col3 = st.columns([0.33, 0.33, 0.33])
        
        with col1:
            # Crear gráfico para Alameda
            # column_to_plot = st.selectbox(
            # "Seleccione la característica a analizar",
            # ["Sociability", "Wellbeing", "Environmental_Impact"])
            value = alameda_data[column_to_plot.lower()]
            gauge_chart = create_gauge_chart(column_to_plot, value)
            # st.plotly_chart(gauge_chart)

            st.markdown(hide_img_fs, unsafe_allow_html=True)

            st.plotly_chart(gauge_chart, use_container_width=True, 
                            use_container_height=True,
                            config = {'displayModeBar': True})
            
            
        with col2:
            # Seleccionar comuna
            # selecciona_comunas = st.selectbox("Seleccione una comuna", comunas_santi["Comuna"].unique(), key='comuna_gauge')
            comuna_selected = comunas_santi[comunas_santi["Comuna"] == selecciona_comunas]
            value = comuna_selected[column_to_plot.lower()].sum()
            gauge_chart = create_gauge_chart(column_to_plot, value)
            # st.plotly_chart(gauge_chart)
            st.markdown(hide_img_fs, unsafe_allow_html=True)

            st.plotly_chart(gauge_chart, use_container_width=True, 
                            use_container_height=True,
                            config = {'displayModeBar': True})
            
        with col3:
            # Seleccionar unidad vecinal
            # selecciona_unidades = st.selectbox("Seleccione una unidad vecinal", unidades_vecinales["COD_UNICO_"].unique(), key='unidad_gauge')
            unidad_selected = unidades_vecinales[unidades_vecinales["COD_UNICO_"] == selecciona_unidades]
            value = unidad_selected[column_to_plot.lower()].sum()
            gauge_chart = create_gauge_chart(column_to_plot, value)
            # st.plotly_chart(gauge_chart)
            st.markdown(hide_img_fs, unsafe_allow_html=True)

            st.plotly_chart(gauge_chart, use_container_width=True, 
                            use_container_height=True,
                            config = {'displayModeBar': True})

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
    comuna_select, uv_select = user_area_selection()
    scatters(comuna_select, uv_select)
    column_to_plot = user_indicator_selection()
    gauges(comuna_select, uv_select, column_to_plot)

# Llamada a la función principal
main()
