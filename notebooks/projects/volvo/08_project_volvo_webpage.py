# Librerías:
import streamlit as st
import folium
from streamlit_folium import st_folium
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 
from matplotlib.patches import Patch
import geopandas as gpd
import os
import random
import plotly.graph_objects as go
import plotly.express as px

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
    ("Mapas", "Hallazgos")
)

# Configuración específica para cada tipo de visualización
if visualizacion == "Mapas":
    # Para mapas: selección individual de ciudad
    ciudad_seleccionada = st.selectbox(
        "Seleccione la ciudad que desea visualizar:", 
        ('Guadalajara', 'Medellín'), 
        key='ciudad_mapas'
    )
    
    @st.cache_data
    def cargar_poligonos_estudio(base_path, ciudad):
        """
        Carga el archivo de polígonos específico para cada ciudad
        """
        try:
            if ciudad == 'Guadalajara':
                ruta_archivo = os.path.join(base_path, "PoligonosEstudio.gpkg")
            else:  # Medellín
                ruta_archivo = os.path.join(base_path, "PoligonoEstudio_MDE.gpkg")
            
            if os.path.exists(ruta_archivo):
                gdf_poligonos = gpd.read_file(ruta_archivo)
                if 'geometry' not in gdf_poligonos.columns or gdf_poligonos.empty:
                    return None
                # Simplificar geometrías para mejorar rendimiento
                gdf_poligonos.geometry = gdf_poligonos.geometry.simplify(0.001)
                return gdf_poligonos
            else:
                return None
        except Exception as e:
            return None

    def cargar_variables_fisicas(ruta_variables_fisicas, ciudad):
        """
        Carga las variables físicas desde la carpeta especificada
        """
        variables_fisicas = {}
        try:
            if os.path.exists(ruta_variables_fisicas):
                for file in os.listdir(ruta_variables_fisicas):
                    if file.endswith('.shp') or file.endswith('.gpkg'):
                        file_path = os.path.join(ruta_variables_fisicas, file)
                        try:
                            gdf = gpd.read_file(file_path)
                            if 'geometry' in gdf.columns and not gdf.empty:
                                if gdf.crs is not None and gdf.crs != 'EPSG:4326':
                                    gdf = gdf.to_crs('EPSG:4326')
                                variables_fisicas[file] = gdf
                        except Exception as e:
                            continue
        except Exception as e:
            pass
        return variables_fisicas

    @st.cache_data
    def cargar_datos_escuelas():
        """
        Carga los datos de escuelas desde el archivo geojson y calcula estadísticas
        Solo carga las columnas necesarias para mejorar rendimiento
        """
        try:
            ruta_archivo = "/home/jovyan/accesibilidad-urbana/data/external/WalkabilityIndex/volvo_wgtproxanalysis_2024_mza_hex9.geojson"
            if os.path.exists(ruta_archivo):
                # Solo leer las columnas necesarias para las estadísticas
                gdf_escuelas = gpd.read_file(ruta_archivo, columns=['max_escuelas', 'min_escuelas'])
                
                # Verificar que las columnas existan
                if 'max_escuelas' in gdf_escuelas.columns and 'min_escuelas' in gdf_escuelas.columns:
                    # Calcular estadísticas para max_escuelas (más eficiente con pandas)
                    max_stats = {
                        'min': float(gdf_escuelas['max_escuelas'].min()),
                        'max': float(gdf_escuelas['max_escuelas'].max()),
                        'mean': float(gdf_escuelas['max_escuelas'].mean())
                    }
                    
                    # Calcular estadísticas para min_escuelas
                    min_stats = {
                        'min': float(gdf_escuelas['min_escuelas'].min()),
                        'max': float(gdf_escuelas['min_escuelas'].max()),
                        'mean': float(gdf_escuelas['min_escuelas'].mean())
                    }
                    
                    return max_stats, min_stats
                else:
                    return None, None
            else:
                return None, None
        except Exception as e:
            return None, None

    def edges_to_map_specific_sectors(base_path, sectores_objetivo, ciudad, ubicacion_centro):
        """
        Carga archivos *edges_proj_net_final.shp de sectores específicos y variables físicas,
        los muestra en un mapa con buffers de 10 metros para las líneas.
        """
        if not os.path.exists(base_path):
            st.error(f"La ruta {base_path} no existe.")
            return

        # Crear mapa centrado en la ciudad correspondiente - MÁS GRANDE
        m = folium.Map(location=ubicacion_centro, zoom_start=13, tiles="cartodbpositron")

        # Cargar variables físicas
        ruta_variables_fisicas = "/home/jovyan/accesibilidad-urbana/data/external/Variables_Físicas"
        variables_fisicas = cargar_variables_fisicas(ruta_variables_fisicas, ciudad)
        
        # Añadir variables físicas al mapa
        for nombre_archivo, gdf_var in variables_fisicas.items():
            try:
                folium.GeoJson(
                    gdf_var,
                    name=f"Variables Físicas - {nombre_archivo.split('.')[0]}",
                    style_function=lambda feature: {
                        'fillColor': 'green',
                        'color': 'darkgreen',
                        'weight': 2,
                        'fillOpacity': 0.3,
                        'opacity': 0.7
                    }
                ).add_to(m)
            except Exception as e:
                continue

        # Cargar polígonos de estudio específicos por ciudad
        gdf_poligonos = cargar_poligonos_estudio(base_path, ciudad)
        
        if gdf_poligonos is not None:
            try:
                folium.GeoJson(
                    gdf_poligonos,
                    name="Polígonos de Estudio",
                    style_function=lambda feature: {
                        'fillColor': 'blue',
                        'color': 'darkblue',
                        'weight': 3,
                        'fillOpacity': 0.2,
                        'opacity': 0.8
                    }
                ).add_to(m)
            except Exception as e:
                pass

        # Paleta de colores para los sectores
        color_palette = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
            "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
        ]

        # Mapeo fijo de sector a color
        colores = {sector: color_palette[i % len(color_palette)] for i, sector in enumerate(sectores_objetivo)}

        sectores_encontrados = []
        
        for sector in sectores_objetivo:
            sector_path = os.path.join(base_path, sector)
            
            if not os.path.exists(sector_path):
                continue
                
            sectores_encontrados.append(sector)

            for file in os.listdir(sector_path):
                # SOLO archivos edges_proj_net_final.shp
                if file.endswith('edges_proj_net_final.shp'):
                    file_path = os.path.join(sector_path, file)
                    try:
                        gdf = gpd.read_file(file_path)
                        
                        if gdf.empty or 'geometry' not in gdf.columns or gdf.geometry.isnull().all():
                            continue
                        
                        # Convertir a sistema de coordenadas apropiado para buffer
                        if gdf.crs is None or gdf.crs == 'EPSG:4326':
                            # Usar UTM apropiado según la ciudad
                            if ciudad == 'Guadalajara':
                                gdf = gdf.to_crs('EPSG:32613')  # UTM Zone 13N para Guadalajara
                            else:  # Medellín
                                gdf = gdf.to_crs('EPSG:32618')  # UTM Zone 18N para Medellín
                        
                        # Crear buffer de 10 metros
                        gdf_buffer = gdf.copy()
                        gdf_buffer.geometry = gdf.geometry.buffer(10)
                        
                        # Convertir de vuelta a WGS84 para el mapa
                        gdf_buffer = gdf_buffer.to_crs('EPSG:4326')

                        # Añadir al mapa con control de capas (ahora como polígonos buffer)
                        folium.GeoJson(
                            gdf_buffer,
                            name=f"{sector}",  # Nombre simplificado
                            style_function=lambda feature, col=colores[sector]: {
                                'color': col,
                                'fillColor': col,
                                'weight': 2,
                                'opacity': 0.8,
                                'fillOpacity': 0.3
                            }
                        ).add_to(m)

                    except Exception as e:
                        continue

        # Añadir control de capas para encender/apagar
        folium.LayerControl(collapsed=False).add_to(m)
        
        # Crear layout en dos columnas
        col1, col2 = st.columns([3, 1])  # 75% mapa, 25% leyenda
        
        with col1:
            if sectores_encontrados:
                sectores_str = ", ".join(sectores_encontrados)
                st.subheader(f"Mapa de líneas - {ciudad} (Sectores: {sectores_str})")
            else:
                st.subheader(f"Mapa de líneas - {ciudad}")
                
            # MAPA MÁS GRANDE
            st_folium(m, width=1200, height=600)
        
        with col2:
            # BARRA DE LEYENDA
            st.markdown("### Leyenda")
            
            # Leyenda de elementos básicos
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
            
            # Separador
            st.markdown("---")
            
            # Cargar datos reales de escuelas
            max_stats, min_stats, gdf_escuelas = cargar_datos_escuelas()
            
            if max_stats is not None and min_stats is not None:
                # Leyenda con datos reales de max_escuelas
                st.markdown(f"""
                    <div style="text-align: center; font-size: 12px; font-weight: bold; margin-bottom: 10px;">
                        Acceso Máximo a Escuelas
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("""
                    <div style="display: flex; align-items: center;">
                        <span style="background: linear-gradient(0deg, rgba(254,235,226,1) 17%, 
                            rgba(252,197,192,1) 34%, rgba(250,159,181,1) 51%, 
                            rgba(247,104,161,1) 68%, 
                            rgba(197,27,138,1) 85%, rgba(122,1,119,1) 100%);
                            display: inline-block; width: 20px; height: 60px;">
                        </span>
                        <div style="margin-left: 10px; font-size: 10px;">
                            <div>{:.1f}</div>
                            <div style="margin-top: 5px;">{:.1f}</div>
                            <div style="margin-top: 5px;">{:.1f}</div>
                            <div style="margin-top: 5px;">{:.1f}</div>
                        </div>
                    </div>
                    """.format(max_stats['max'], max_stats['mean'], max_stats['mean']/2, max_stats['min']), 
                    unsafe_allow_html=True)
                
                st.markdown("---")
                
                # Leyenda con datos reales de min_escuelas
                st.markdown(f"""
                    <div style="text-align: center; font-size: 12px; font-weight: bold; margin-bottom: 10px;">
                        Acceso Mínimo a Escuelas
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("""
                    <div style="display: flex; align-items: center;">
                        <span style="background: linear-gradient(0deg, rgba(158,1,66,1) 10%, 
                            rgba(213,62,79,1) 20%, rgba(244,109,67,1) 30%, 
                            rgba(253,174,97,1) 40%, rgba(254,224,139,1) 50%, 
                            rgba(230,245,152,1) 60%, rgba(171,221,164,1) 70%, 
                            rgba(102,194,165,1) 80%, rgba(50,136,189,1) 90%, 
                            rgba(94,79,162,1) 100%);
                            display: inline-block; width: 20px; height: 60px;">
                        </span>
                        <div style="margin-left: 10px; font-size: 10px;">
                            <div>{:.1f}</div>
                            <div style="margin-top: 5px;">{:.1f}</div>
                            <div style="margin-top: 5px;">{:.1f}</div>
                            <div style="margin-top: 5px;">{:.1f}</div>
                        </div>
                    </div>
                    """.format(min_stats['max'], min_stats['mean'], min_stats['mean']/2, min_stats['min']), 
                    unsafe_allow_html=True)
                
                # Mostrar estadísticas adicionales
                st.markdown("---")
                st.markdown("### Estadísticas")
                st.markdown(f"""
                    <div style="font-size: 10px;">
                        <strong>Max Escuelas:</strong><br>
                        Rango: {max_stats['min']:.1f} - {max_stats['max']:.1f}<br>
                        Promedio: {max_stats['mean']:.1f}<br><br>
                        <strong>Min Escuelas:</strong><br>
                        Rango: {min_stats['min']:.1f} - {min_stats['max']:.1f}<br>
                        Promedio: {min_stats['mean']:.1f}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                # Fallback a leyendas estáticas si no se pueden cargar los datos
                st.markdown("""
                    <div style="text-align: center; font-size: 12px; font-weight: bold; margin-bottom: 10px;">
                        Datos no disponibles - Mostrando leyenda estática
                    </div>
                    """, unsafe_allow_html=True)
            
            # Leyenda de sectores con colores
            st.markdown("---")
            st.markdown("### Sectores")
            for sector in sectores_encontrados:
                color = colores[sector]
                st.markdown(f"""
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <span style="background-color: {color}; width: 15px; height: 3px; display: inline-block;"></span>
                        <span style="margin-left: 8px; font-size: 12px;">{sector}</span>
                    </div>
                    """, unsafe_allow_html=True)

    # Configuración de sectores por ciudad
    sectores_config = {
        'Guadalajara': {
            'sectores': ['colinas_HL', 'miramar_LH', 'providencia_HH'],
            'centro': [20.6736, -103.344]
        },
        'Medellín': {
            'sectores': ['aguacatala_HL', 'floresta_HH', 'moravia_LH'],
            'centro': [6.2442, -75.5812]
        }
    }

    # Mostrar mapa de la ciudad seleccionada
    st.sidebar.title("Opciones del mapa")
    ver_mapa = st.sidebar.checkbox("Mostrar mapas de líneas por sector", value=True)

    if ver_mapa:
        base_path = "/home/jovyan/accesibilidad-urbana/data/external/WalkabilityIndex/"
        config = sectores_config[ciudad_seleccionada]
        edges_to_map_specific_sectors(
            base_path, 
            config['sectores'], 
            ciudad_seleccionada, 
            config['centro']
        )


# Configuración de estilo para matplotlib
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (8, 6)
plt.rcParams['font.size'] = 10

def limpiar_datos(df):
    """Limpia los datos de caracteres invisibles y problemas de formato"""
    df_clean = df.copy()
    
    # Limpiar todas las columnas
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            # Remover caracteres invisibles específicos
            df_clean[col] = df_clean[col].astype(str).str.replace('​', '', regex=False)  # Carácter invisible específico
            df_clean[col] = df_clean[col].str.replace('\u200b', '', regex=False)  # Zero-width space
            df_clean[col] = df_clean[col].str.replace('\ufeff', '', regex=False)  # BOM
            df_clean[col] = df_clean[col].str.replace('\xa0', ' ', regex=False)   # Non-breaking space
            df_clean[col] = df_clean[col].str.replace('-', '0', regex=False)      # Reemplazar guiones por 0
            df_clean[col] = df_clean[col].str.strip()  # Espacios al inicio/final
            
    return df_clean

def convertir_a_float_seguro(serie):
    """Convierte datos a float de manera segura, manejando errores"""
    try:
        # Limpiar la serie antes de convertir
        serie_clean = serie.astype(str).str.replace('​', '', regex=False)
        serie_clean = serie_clean.str.replace('\u200b', '', regex=False)
        serie_clean = serie_clean.str.replace('\ufeff', '', regex=False)
        serie_clean = serie_clean.str.replace('\xa0', '', regex=False)
        serie_clean = serie_clean.str.replace('-', '0', regex=False)
        serie_clean = serie_clean.str.replace('nan', '0', regex=False)
        serie_clean = serie_clean.str.strip()
        
        # Reemplazar strings vacíos con 0
        serie_clean = serie_clean.replace('', '0')
        serie_clean = serie_clean.replace('None', '0')
        
        return pd.to_numeric(serie_clean, errors='coerce').fillna(0)
    except Exception as e:
        st.warning(f"Problema al convertir datos: {e}")
        return pd.Series([0] * len(serie))

@st.cache_data
def cargar_datos():
    """Carga los datos necesarios para la visualización"""
    try:
        betas_GDL = pd.read_excel("/home/jovyan/accesibilidad-urbana/data/external/BETAS_GDL_MDE.xlsx", sheet_name="GDL")
        betas_MDE = pd.read_excel("/home/jovyan/accesibilidad-urbana/data/external/BETAS_GDL_MDE.xlsx", sheet_name="Medellin")
        hallazgos_vref = pd.read_csv("/home/jovyan/accesibilidad-urbana/data/external/hallazgos_vref_clean.csv", encoding='utf-8')
        
        # Limpiar datos de caracteres invisibles
        hallazgos_vref = limpiar_datos(hallazgos_vref)
        
        return betas_GDL, betas_MDE, hallazgos_vref
    except Exception as e:
        st.error(f"Error al cargar los datos: {e}")
        return None, None, None

def extraer_fila_por_concepto(df, concepto_buscar):
    """Extrae una fila específica basada en el concepto en la primera o segunda columna"""
    # Limpiar el concepto que estamos buscando
    concepto_limpio = concepto_buscar.replace('​', '').replace('\u200b', '').replace('\ufeff', '').replace('\xa0', ' ').strip()
    
    # Buscar en primera columna
    primera_col = df.iloc[:, 0].astype(str).str.replace('​', '', regex=False).str.strip()
    mask_primera = primera_col.str.contains(concepto_limpio, case=False, na=False, regex=False)
    
    if mask_primera.any():
        fila_idx = mask_primera.idxmax()
        return df.iloc[fila_idx, :]
    
    # Si no encuentra en primera columna, buscar en segunda columna
    if df.shape[1] > 1:
        segunda_col = df.iloc[:, 1].astype(str).str.replace('​', '', regex=False).str.strip()
        mask_segunda = segunda_col.str.contains(concepto_limpio, case=False, na=False, regex=False)
        
        if mask_segunda.any():
            fila_idx = mask_segunda.idxmax()
            return df.iloc[fila_idx, :]
    
    return None

def obtener_indice_zona(zona, colonias_gdl, colonias_mde, titulo_ciudad):
    """Obtiene el índice de columna correspondiente a la zona seleccionada"""
    if titulo_ciudad == "Guadalajara":
        # Columnas 1, 2, 3 para Guadalajara (Colinas, Providencia, Miramar)
        return colonias_gdl.index(zona) + 1
    else:  # Medellín
        # Columnas 5, 6, 7 para Medellín (Aguacatala, Floresta, Moravia)
        return colonias_mde.index(zona) + 5

def crear_grafico_genero_dona(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de dona de distribución por género para la zona seleccionada"""
    
    # Extraer datos de % Mujeres
    fila_mujeres = extraer_fila_por_concepto(df, "% Mujeres")
    
    if fila_mujeres is not None:
        # Obtener índice de la zona seleccionada
        indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
        
        # Obtener porcentaje de mujeres para la zona seleccionada
        porcentaje_mujeres = convertir_a_float_seguro(pd.Series([fila_mujeres.iloc[indice_zona]])).iloc[0]
        
        # Asegurar que el valor no sea negativo y esté en rango válido
        porcentaje_mujeres = max(0, min(100, porcentaje_mujeres))
        porcentaje_hombres = 100 - porcentaje_mujeres
        
        # Verificar que tengamos datos válidos
        if porcentaje_mujeres == 0 and porcentaje_hombres == 0:
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.text(0.5, 0.5, 'Datos no válidos para género en esta zona', 
                   transform=ax.transAxes, ha='center', va='center', fontsize=14)
            ax.set_title(f'Distribución por Género - {zona_seleccionada}', fontsize=16)
            ax.axis('off')
            return fig
        
        # Crear gráfico de dona
        fig, ax = plt.subplots(figsize=(8, 8))
        
        # Datos para el gráfico
        datos = [porcentaje_mujeres, porcentaje_hombres]
        etiquetas = ['Mujeres', 'Hombres']
        colores = ['#E91E63', '#2196F3']  # Rosa para mujeres, azul para hombres
        
        # Crear gráfico de dona (pie con hole)
        wedges, texts, autotexts = ax.pie(
            datos, 
            labels=etiquetas, 
            colors=colores,
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 12},
            wedgeprops={'width': 0.5}  # Esto crea el efecto de dona
        )
        
        # Mejorar el texto de los porcentajes
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(14)
        
        ax.set_title(f'Distribución por Género - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
    else:
        # Si no hay datos, crear un gráfico simple con mensaje
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no encontrados para % Mujeres', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Distribución por Género - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
    
    return fig

def crear_grafico_vehiculos_dona(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de dona de tenencia de vehículos para la zona seleccionada"""
    
    # Extraer datos de vehículos
    fila_auto = extraer_fila_por_concepto(df, "% con auto")
    fila_auto_moto = extraer_fila_por_concepto(df, "% con auto/moto")
    
    if fila_auto is not None and fila_auto_moto is not None:
        # Obtener índice de la zona seleccionada
        indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
        
        # Obtener porcentajes para la zona seleccionada
        porcentaje_auto = convertir_a_float_seguro(pd.Series([fila_auto.iloc[indice_zona]])).iloc[0]
        porcentaje_auto_moto = convertir_a_float_seguro(pd.Series([fila_auto_moto.iloc[indice_zona]])).iloc[0]
        
        # Asegurar que los valores no sean negativos
        porcentaje_auto = max(0, porcentaje_auto)
        porcentaje_auto_moto = max(0, porcentaje_auto_moto)
        
        # Calcular total con vehículo y sin vehículo
        total_con_vehiculo = porcentaje_auto + porcentaje_auto_moto
        porcentaje_sin_vehiculo = max(0, 100 - total_con_vehiculo)
        
        # Verificar que tengamos datos válidos
        if total_con_vehiculo + porcentaje_sin_vehiculo == 0:
            # Si todos los valores son 0, mostrar mensaje de error
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.text(0.5, 0.5, 'Datos no válidos para vehículos en esta zona', 
                   transform=ax.transAxes, ha='center', va='center', fontsize=14)
            ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}', fontsize=16)
            ax.axis('off')
            return fig
        
        # Crear gráfico de dona
        fig, ax = plt.subplots(figsize=(8, 8))
        
        # Datos para el gráfico (filtrar valores cero para mejor visualización)
        datos = []
        etiquetas = []
        colores_usados = []
        colores_disponibles = ['#4CAF50', '#FF9800', '#F44336']  # Verde, naranja, rojo
        etiquetas_disponibles = ['Solo Auto', 'Auto + Moto', 'Sin Vehículo']
        valores_disponibles = [porcentaje_auto, porcentaje_auto_moto, porcentaje_sin_vehiculo]
        
        for i, valor in enumerate(valores_disponibles):
            if valor > 0:  # Solo incluir valores positivos
                datos.append(valor)
                etiquetas.append(etiquetas_disponibles[i])
                colores_usados.append(colores_disponibles[i])
        
        # Crear gráfico de dona
        wedges, texts, autotexts = ax.pie(
            datos, 
            labels=etiquetas, 
            colors=colores_usados,
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 12},
            wedgeprops={'width': 0.5}  # Esto crea el efecto de dona
        )
        
        # Mejorar el texto de los porcentajes
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(14)
        
        ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
    else:
        # Si no hay datos, crear un gráfico simple con mensaje
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no encontrados para vehículos', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
    
    return fig

def crear_grafico_transporte_horizontal(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de barras horizontales de medios de transporte para la zona seleccionada"""
    
    # Extraer datos de medios de transporte
    medios = ['Caminata', 'Auto', 'Bus / SITVA', 'Motocicleta']
    datos_transporte = []
    
    # Obtener índice de la zona seleccionada
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    for medio in medios:
        fila = extraer_fila_por_concepto(df, medio)
        if fila is not None:
            valor = convertir_a_float_seguro(pd.Series([fila.iloc[indice_zona]])).iloc[0]
            datos_transporte.append(valor)
        else:
            datos_transporte.append(0)
    
    if len(datos_transporte) > 0:
        # Crear gráfico de barras horizontales
        fig, ax = plt.subplots(figsize=(10, 6))
        
        colores = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        barras = ax.barh(medios, datos_transporte, color=colores)
        
        # Añadir valores en las barras
        for i, (barra, valor) in enumerate(zip(barras, datos_transporte)):
            ax.text(barra.get_width() + 1, barra.get_y() + barra.get_height()/2, 
                   f'{valor:.1f}%', va='center', fontweight='bold')
        
        ax.set_xlabel('Porcentaje (%)', fontsize=12)
        ax.set_title(f'Medios de Transporte Utilizados - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        
    else:
        # Si no hay datos, crear un gráfico simple con mensaje
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no encontrados para transporte', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Medios de Transporte Utilizados - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
    
    return fig

def crear_grafico_razones_horizontal(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de barras horizontales de razones para caminar para la zona seleccionada"""
    
    # Razones para caminar
    razones = ['Cercanía al lugar', 'Por salud', 'Única alternativa', 
               'Distracción /desestrés/ Gusto /Apreciación', 'Ahorrar tiempo', 'No tengo carro']
    nombres_cortos = ['Cercanía', 'Salud', 'Única alternativa', 'Gusto', 'Ahorrar tiempo', 'No tengo carro']
    
    datos_razones = []
    
    # Obtener índice de la zona seleccionada
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    for razon in razones:
        fila = extraer_fila_por_concepto(df, razon)
        if fila is not None:
            valor = convertir_a_float_seguro(pd.Series([fila.iloc[indice_zona]])).iloc[0]
            datos_razones.append(valor)
        else:
            datos_razones.append(0)
    
    if len(datos_razones) > 0:
        # Crear gráfico de barras horizontales
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Usar solo los nombres cortos que corresponden a los datos encontrados
        nombres_usados = nombres_cortos[:len(datos_razones)]
        
        colores = plt.cm.viridis(np.linspace(0, 1, len(datos_razones)))
        barras = ax.barh(nombres_usados, datos_razones, color=colores)
        
        # Añadir valores en las barras
        for i, (barra, valor) in enumerate(zip(barras, datos_razones)):
            ax.text(barra.get_width() + 0.5, barra.get_y() + barra.get_height()/2, 
                   f'{valor:.1f}%', va='center', fontweight='bold')
        
        ax.set_xlabel('Porcentaje (%)', fontsize=12)
        ax.set_title(f'Razones para Caminar - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        
    else:
        # Si no hay datos, crear un gráfico simple con mensaje
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no encontrados para razones', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Razones para Caminar - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
    
    return fig

# No necesitamos esta función ya que se eliminó el gráfico de barras

def crear_grafico_radar(betas_df, zona_seleccionada, colores_dict):
    """Crea gráfico radar para una zona específica"""
    categorias = betas_df['Variable'].tolist()
    valores = betas_df[zona_seleccionada].tolist()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=valores,
        theta=categorias,
        fill='toself',
        name=zona_seleccionada,
        line_color=colores_dict.get(zona_seleccionada, '#000000')
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[-2, 2])
        ),
        showlegend=False,
        title=f"Perfil de caminabilidad: {zona_seleccionada}",
        font=dict(size=12)
    )
    
    return fig

def mostrar_seccion_ciudad(df, betas_df, titulo_ciudad, colonias_gdl, colonias_mde, zonas, colores_dict):
    """Muestra todos los gráficos para una ciudad específica con selección de zona"""
    st.subheader(f"Hallazgos de la investigación en {titulo_ciudad}")
    st.markdown(f"""En esta sección se presentan los hallazgos más relevantes de la investigación, 
    basados en las encuestas realizadas para ciudad de {titulo_ciudad}. Estos hallazgos se centran 
    en la percepción de caminabilidad y la calidad del espacio público en diferentes colonias de la ciudad.""")
    
    # Selector de zona para toda la ciudad
    zona_key = f"zona_{titulo_ciudad.lower().replace(' ', '_')}"
    zona_seleccionada = st.selectbox(f"Selecciona una zona de {titulo_ciudad}", zonas, key=zona_key)
    
    # Gráficos demográficos y de comportamiento para la zona seleccionada
    st.markdown("#### Distribución por Género")
    st.pyplot(crear_grafico_genero_dona(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Tenencia de Vehículos")
    st.pyplot(crear_grafico_vehiculos_dona(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Medios de Transporte Utilizados")
    st.pyplot(crear_grafico_transporte_horizontal(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Razones para Caminar")
    st.pyplot(crear_grafico_razones_horizontal(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    # Sección de betas - solo gráfico radar
    st.subheader(f"Perfil de Caminabilidad - {titulo_ciudad}")
    st.markdown("Los betas son coeficientes que indican la relación entre las variables y el índice de caminabilidad. Van de -2 a 2, donde valores negativos indican una relación inversa y positivos una relación directa.")
    
    # Gráfico radar (usa la misma zona seleccionada)
    fig_radar = crear_grafico_radar(betas_df, zona_seleccionada, colores_dict)
    st.plotly_chart(fig_radar, use_container_width=True)

def crear_graficos(hallazgos_vref, betas_GDL, betas_MDE):
    """Función principal que organiza toda la visualización"""
    
    # Nombres de las zonas
    colonias_guadalajara = ["Colinas", "Providencia", "Miramar"]
    colonias_medellin = ["Aguacatala", "Floresta", "Moravia"]
    
    # Diccionarios de colores para gráficos radar
    colores_gdl = {
        "Colinas": '#E9AEFA',
        "Providencia": '#b18fbb',
        "Miramar": '#9769a4'
    }
    
    colores_mde = {
        "Aguacatala": '#E9AEFA',
        "Floresta": '#b18fbb',  
        "Moravia": '#9769a4'
    }
    
    # Layout de dos columnas
    col1, col2 = st.columns(2)
    
    with col1:
        mostrar_seccion_ciudad(
            hallazgos_vref,
            betas_GDL, 
            "Guadalajara",
            colonias_guadalajara,
            colonias_medellin,
            colonias_guadalajara,
            colores_gdl
        )
    
    with col2:
        mostrar_seccion_ciudad(
            hallazgos_vref,
            betas_MDE, 
            "Medellín",
            colonias_guadalajara,
            colonias_medellin,
            colonias_medellin,
            colores_mde
        )

# Función principal para el flujo de la aplicación
if visualizacion == "Hallazgos":
    # Cargar datos con cache
    betas_GDL, betas_MDE, hallazgos_vref = cargar_datos()
    
    if all([betas_GDL is not None, betas_MDE is not None, hallazgos_vref is not None]):
        crear_graficos(hallazgos_vref, betas_GDL, betas_MDE)
    else:
        st.error("No se pudieron cargar los datos necesarios para la visualización.")