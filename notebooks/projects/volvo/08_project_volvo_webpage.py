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
    
    
    @st.cache_data(ttl=3600, show_spinner="Cargando polígonos de estudio...")
    def cargar_poligonos_estudio(base_path, ciudad):
        """Carga y simplifica los polígonos de estudio para cada ciudad"""
        try:
            archivo = "PolígonosEstudio.gpkg" if ciudad == 'Guadalajara' else "PoligonoEstudio_MDE.gpkg"
            ruta = os.path.join(base_path, archivo)
            
            if not os.path.exists(ruta):
                st.warning(f"Archivo no encontrado: {ruta}")
                return None
                
            gdf = gpd.read_file(ruta)
            if gdf.empty:
                return None
                
            # Simplificación geométrica para mejorar rendimiento
            gdf['geometry'] = gdf.geometry.simplify(tolerance=0.0005, preserve_topology=True)
            return gdf
        
        except Exception as e:
            st.error(f"Error cargando polígonos: {str(e)}")
            return None

    @st.cache_data(ttl=3600)
    def cargar_variables_fisicas(ruta_base, ciudad):
        """Carga variables físicas con filtrado por ciudad"""
        try:
            variables = {}
            if not os.path.exists(ruta_base):
                return variables
                
            for archivo in os.listdir(ruta_base):
                if archivo.endswith(('.shp', '.gpkg')):
                    ruta = os.path.join(ruta_base, archivo)
                    try:
                        gdf = gpd.read_file(ruta, rows=1000)  # Limitar datos para prueba
                        if not gdf.empty and 'geometry' in gdf.columns:
                            if gdf.crs != 'EPSG:4326':
                                gdf = gdf.to_crs('EPSG:4326')
                            variables[archivo] = gdf
                    except Exception as e:
                        continue
            return variables
        except Exception as e:
            st.error(f"Error cargando variables físicas: {str(e)}")
            return {}

    @st.cache_data(ttl=3600)
    def cargar_datos_escuelas():
        """Carga datos de escuelas con estadísticas básicas"""
        try:
            ruta = "/home/jovyan/accesibilidad-urbana/data/external/WalkabilityIndex/volvo_wgtproxanalysis_2024_mza_hex9.geojson"
            if not os.path.exists(ruta):
                return None, None
                
            columnas = ['max_escuelas', 'min_escuelas']
            gdf = gpd.read_file(ruta, columns=columnas)
            
            if not all(col in gdf.columns for col in columnas):
                return None, None
                
            max_stats = {
                'min': float(gdf['max_escuelas'].min()),
                'max': float(gdf['max_escuelas'].max()),
                'mean': float(gdf['max_escuelas'].mean())
            }
            
            min_stats = {
                'min': float(gdf['min_escuelas'].min()),
                'max': float(gdf['min_escuelas'].max()),
                'mean': float(gdf['min_escuelas'].mean())
            }
            
            return max_stats, min_stats
            
        except Exception as e:
            st.error(f"Error cargando datos de escuelas: {str(e)}")
            return None, None

    def cargar_edges_sector(sector_path, ciudad, simplificar=True):
        """Carga datos de edges para un sector específico"""
        try:
            archivos = [f for f in os.listdir(sector_path) if f.endswith('edges_proj_net_final.shp')]
            if not archivos:
                return None
                
            gdf = gpd.read_file(os.path.join(sector_path, archivos[0]), rows=500)
            if gdf.empty:
                return None
                
            # Conversión CRS y simplificación
            target_crs = 'EPSG:32613' if ciudad == 'Guadalajara' else 'EPSG:32618'
            gdf = gdf.to_crs(target_crs)
            
            if simplificar:
                gdf['geometry'] = gdf.geometry.simplify(tolerance=1)
                
            return gdf
            
        except Exception as e:
            st.error(f"Error cargando edges para {sector_path}: {str(e)}")
            return None

    # --- Interfaz de usuario ---

    def mostrar_leyenda(sectores, colores, stats_escuelas):
        """Muestra la leyenda del mapa"""
        with st.expander("📌 Leyenda del Mapa", expanded=True):
            st.markdown("### Sectores")
            for i, sector in enumerate(sectores):
                st.markdown(f"""
                    <div style="display:flex; align-items:center; margin:5px 0;">
                        <div style="width:20px; height:10px; background:{colores[i]}; margin-right:10px;"></div>
                        <span>{sector}</span>
                    </div>
                """, unsafe_allow_html=True)
            
            if stats_escuelas and stats_escuelas[0]:
                st.markdown("---")
                st.markdown("### Estadísticas Escuelas")
                st.metric("Máximo promedio", f"{stats_escuelas[0]['mean']:.1f}")
                st.metric("Mínimo promedio", f"{stats_escuelas[1]['mean']:.1f}")

    def configurar_mapa_base(ciudad):
        """Configura el mapa base Folium"""
        centro = [20.6736, -103.344] if ciudad == 'Guadalajara' else [6.2442, -75.5812]
        return folium.Map(
            location=centro,
            zoom_start=13,
            tiles="cartodbpositron",
            control_scale=True
        )

    def mostrar_mapa_completo():
        """Función principal para mostrar el mapa"""
        # st.title("📊 Mapas de Accesibilidad Urbana")
        
        # Configuración de sidebar
        with st.sidebar:
            st.header("Configuración")
            ciudad = st.radio(
                "Seleccione ciudad:",
                ('Guadalajara', 'Medellín'),
                index=0
            )
            
            sectores_disponibles = {
                'Guadalajara': ['colinas_HL', 'miramar_LH', 'providencia_HH'],
                'Medellín': ['aguacatala_HL', 'floresta_HH', 'moravia_LH']
            }
            
            sectores = st.multiselect(
                "Sectores a visualizar:",
                options=sectores_disponibles[ciudad],
                default=[sectores_disponibles[ciudad][0]]
            )
            
            with st.expander("Opciones avanzadas"):
                buffer_size = st.slider("Tamaño de buffer (metros):", 1, 20, 5)
                mostrar_variables = st.checkbox("Mostrar variables físicas", True)
        
        if not sectores:
            st.warning("Seleccione al menos un sector para visualizar")
            return

        # Cargar datos
        base_path = "/home/jovyan/accesibilidad-urbana/data/external/WalkabilityIndex/"
        
        with st.spinner("Cargando datos..."):
            # 1. Configurar mapa base
            m = configurar_mapa_base(ciudad)
            
            # 2. Cargar y añadir polígonos de estudio
            gdf_poligonos = cargar_poligonos_estudio(base_path, ciudad)
            if gdf_poligonos is not None:
                folium.GeoJson(
                    gdf_poligonos,
                    name="Polígonos de Estudio",
                    style_function=lambda x: {
                        'fillColor': '#1a73e8',
                        'color': '#1a73e8',
                        'weight': 2,
                        'fillOpacity': 0.1,
                        'opacity': 0.7
                    }
                ).add_to(m)
            
            # 3. Cargar variables físicas si está habilitado
            if mostrar_variables:
                variables = cargar_variables_fisicas(
                    "/home/jovyan/accesibilidad-urbana/data/external/Variables_Físicas", 
                    ciudad
                )
                for nombre, gdf in variables.items():
                    folium.GeoJson(
                        gdf,
                        name=f"Variable: {nombre.split('.')[0]}",
                        style_function=lambda x: {
                            'fillColor': '#0b8043',
                            'color': '#0b8043',
                            'weight': 1,
                            'fillOpacity': 0.2,
                            'opacity': 0.5
                        }
                    ).add_to(m)
            
            # 4. Cargar y añadir sectores seleccionados
            colores = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
            sectores_cargados = []
            
            for i, sector in enumerate(sectores):
                sector_path = os.path.join(base_path, sector)
                gdf_edges = cargar_edges_sector(sector_path, ciudad)
                
                if gdf_edges is not None:
                    # Crear buffer y convertir a WGS84
                    gdf_buffer = gdf_edges.copy()
                    gdf_buffer['geometry'] = gdf_edges.geometry.buffer(buffer_size)
                    gdf_buffer = gdf_buffer.to_crs('EPSG:4326')
                    
                    # Añadir al mapa
                    folium.GeoJson(
                        gdf_buffer,
                        name=sector,
                        style_function=lambda x, color=colores[i % len(colores)]: {
                            'color': color,
                            'fillColor': color,
                            'weight': 1,
                            'opacity': 0.7,
                            'fillOpacity': 0.2
                        }
                    ).add_to(m)
                    
                    sectores_cargados.append(sector)
            
            # 5. Añadir control de capas
            folium.LayerControl(collapsed=False).add_to(m)
            
            # 6. Cargar datos de escuelas para la leyenda
            stats_escuelas = cargar_datos_escuelas()
        
        # Mostrar mapa y leyenda
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st_folium(m, width=900, height=600, returned_objects=[])
        
        with col2:
            mostrar_leyenda(sectores_cargados, colores, stats_escuelas)

    # --- Punto de entrada principal ---
    if __name__ == "__main__":
        mostrar_mapa_completo()# Cargar datos de geometría


plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (8, 6)
plt.rcParams['font.size'] = 10

@st.cache_data
def cargar_datos():
    """Carga los datos necesarios para la visualización"""
    try:
        betas_GDL = pd.read_excel("/home/jovyan/accesibilidad-urbana/data/external/BETAS_GDL_MDE.xlsx", sheet_name="GDL")
        betas_MDE = pd.read_excel("/home/jovyan/accesibilidad-urbana/data/external/BETAS_GDL_MDE.xlsx", sheet_name="Medellin")
        hallazgos_vref = pd.read_csv("/home/jovyan/accesibilidad-urbana/data/external/hallazgos_vref_limpio.csv", encoding='utf-8')
        
        # Limpiar datos de caracteres invisibles
        hallazgos_vref = limpiar_datos(hallazgos_vref)
        
        return betas_GDL, betas_MDE, hallazgos_vref
    except Exception as e:
        st.error(f"Error al cargar los datos: {e}")
        return None, None, None

def limpiar_datos(df):
    """Limpia caracteres invisibles de todo el DataFrame"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace('​', '', regex=False).str.replace('\u200b', '').str.replace('\ufeff', '').str.replace('\xa0', ' ').str.strip()
    return df

def extraer_fila_por_concepto(df, concepto_buscar):
    """Extrae una fila específica basada en el concepto en la primera o segunda columna"""
    concepto_limpio = concepto_buscar.replace('​', '').replace('\u200b', '').replace('\ufeff', '').replace('\xa0', ' ').strip()
    
    # Buscar en la primera columna (índice 0) o segunda columna (índice 1)
    for col_idx in [0, 1]:
        if col_idx < df.shape[1]:
            col_data = df.iloc[:, col_idx].astype(str).str.replace('​', '', regex=False).str.strip()
            mask = col_data.str.contains(concepto_limpio, case=False, na=False, regex=False)
            
            if mask.any():
                fila_idx = mask.idxmax()
                return df.iloc[fila_idx, :]
    
    return None

def obtener_indice_zona(zona, colonias_gdl, colonias_mde, titulo_ciudad):
    """Obtiene el índice de columna correspondiente a la zona seleccionada"""
    if titulo_ciudad == "Guadalajara":
        # Columnas 1, 2, 3 para Guadalajara (Colinas, Providencia, Miramar)
        return colonias_gdl.index(zona) + 1
    else:  # Medellín
        # Columnas 4, 5, 6 para Medellín (Aguacatala, Floresta, Moravia)
        return colonias_mde.index(zona) + 4

def convertir_a_float_seguro(serie):
    """Convierte valores a float de manera segura"""
    def convertir_valor(val):
        if pd.isna(val) or val == '' or val == 'nan':
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0
    
    return serie.apply(convertir_valor)

def obtener_valor_seguro(fila, indice_zona):
    """Obtiene un valor de manera segura manejando diferentes tipos de índices"""
    try:
        if fila is None or indice_zona >= len(fila):
            return 0
            
        valor_raw = fila.iloc[indice_zona]
        return convertir_a_float_seguro(pd.Series([valor_raw])).iloc[0]
        
    except (IndexError, ValueError, TypeError):
        return 0

def grafico_genero(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de dona con distribución por género y movilidad"""
    
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    # Buscar datos específicos del CSV
    fila_mujeres_caminan = extraer_fila_por_concepto(df, "Mujeres que caminan")
    fila_caminata = extraer_fila_por_concepto(df, "Caminata")
    fila_auto_moto = extraer_fila_por_concepto(df, "Cuentan con auto/moto")
    
    # Obtener valores
    porcentaje_mujeres_caminan = obtener_valor_seguro(fila_mujeres_caminan, indice_zona)
    porcentaje_caminata = obtener_valor_seguro(fila_caminata, indice_zona)
    porcentaje_auto_moto = obtener_valor_seguro(fila_auto_moto, indice_zona)
    
    # Verificar si hay datos válidos
    if porcentaje_caminata == 0 and porcentaje_auto_moto == 0:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no disponibles para esta zona', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.axis('off')
        return fig
    
    # Crear gráfico de dona
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Datos para el gráfico
    datos = [porcentaje_caminata, 100 - porcentaje_caminata]
    etiquetas = ['Camina', 'No camina']
    colores = ['#4CAF50', '#FF5722']
    
    wedges, texts, autotexts = ax.pie(
        datos, 
        labels=etiquetas, 
        colors=colores,
        autopct='%1.1f%%',
        startangle=90,
        textprops={'fontsize': 12, 'fontweight': 'bold'},
        wedgeprops={'width': 0.5}
    )
    
    # Mejorar el texto de los porcentajes
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(14)
    
    # Añadir información adicional en el centro
    if porcentaje_mujeres_caminan > 0:
        ax.text(0, 0, f'Mujeres que caminan:\n{porcentaje_mujeres_caminan:.1f}%', 
               ha='center', va='center', fontsize=12, fontweight='bold')
    
    ax.set_title(f'Distribución de Movilidad - {zona_seleccionada}', 
                fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    return fig

def grafico_vehiculos(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de dona de tenencia de vehículos para la zona seleccionada"""
    
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    # Extraer datos de vehículos
    fila_auto = extraer_fila_por_concepto(df, "Cuentan con auto")
    fila_auto_moto = extraer_fila_por_concepto(df, "Cuentan con auto/moto")
    
    if fila_auto is not None and fila_auto_moto is not None:
        porcentaje_auto = obtener_valor_seguro(fila_auto, indice_zona)
        porcentaje_auto_moto = obtener_valor_seguro(fila_auto_moto, indice_zona)
        
        # Calcular porcentaje solo moto (auto/moto - auto)
        porcentaje_solo_moto = max(0, porcentaje_auto_moto - porcentaje_auto)
        porcentaje_sin_vehiculo = max(0, 100 - porcentaje_auto_moto)
        
        # Verificar datos válidos
        if porcentaje_auto + porcentaje_solo_moto + porcentaje_sin_vehiculo == 0:
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.text(0.5, 0.5, 'Datos no disponibles para vehículos', 
                   transform=ax.transAxes, ha='center', va='center', fontsize=14)
            ax.axis('off')
            return fig
        
        # Crear gráfico
        fig, ax = plt.subplots(figsize=(8, 8))
        
        datos = [porcentaje_auto, porcentaje_solo_moto, porcentaje_sin_vehiculo]
        etiquetas = ['Solo Auto', 'Solo Moto', 'Sin Vehículo']
        colores = ['#2196F3', '#FF9800', '#F44336']
        
        # Filtrar valores cero
        datos_filtrados = []
        etiquetas_filtradas = []
        colores_filtrados = []
        
        for d, e, c in zip(datos, etiquetas, colores):
            if d > 0:
                datos_filtrados.append(d)
                etiquetas_filtradas.append(e)
                colores_filtrados.append(c)
        
        wedges, texts, autotexts = ax.pie(
            datos_filtrados, 
            labels=etiquetas_filtradas, 
            colors=colores_filtrados,
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 12, 'fontweight': 'bold'},
            wedgeprops={'width': 0.5}
        )
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(14)
        
        ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
    else:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no encontrados para vehículos', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.axis('off')
    
    return fig

def grafico_transporte(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de barras horizontales de medios de transporte"""
    
    # Medios de transporte según el CSV
    medios = ['Caminata', 'Auto', 'Bus / SITVA']
    datos_transporte = []
    
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    for medio in medios:
        fila = extraer_fila_por_concepto(df, medio)
        valor = obtener_valor_seguro(fila, indice_zona)
        datos_transporte.append(valor)
    
    if sum(datos_transporte) > 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        colores = ['#1f77b4', '#ff7f0e', '#2ca02c']
        barras = ax.barh(medios, datos_transporte, color=colores)
        
        # Añadir valores en las barras
        for barra, valor in zip(barras, datos_transporte):
            if valor > 0:  # Solo mostrar valores mayores a 0
                ax.text(barra.get_width() + max(datos_transporte) * 0.01, 
                       barra.get_y() + barra.get_height()/2, 
                       f'{valor:.1f}%', va='center', fontweight='bold')
        
        ax.set_xlabel('Porcentaje (%)', fontsize=12)
        ax.set_title(f'Medios de Transporte - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        ax.set_xlim(0, max(datos_transporte) * 1.15)
        
        plt.tight_layout()
        
    else:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no disponibles para transporte', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.axis('off')
    
    return fig

def grafico_razones(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de barra horizontal apilada de razones para caminar"""
    
    # Razones según el CSV
    razones = ['Cercania al lugar', 'Por salud', 'Unica alternativa', 
               'Distraccion /desestres/ Gusto /Apreciacion', 'Ahorrar tiempo', 'No tengo carro']
    nombres_cortos = ['Cercanía', 'Salud', 'Única alternativa', 'Gusto', 'Ahorrar tiempo', 'No tengo carro']
    
    datos_razones = []
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    for razon in razones:
        fila = extraer_fila_por_concepto(df, razon)
        valor = obtener_valor_seguro(fila, indice_zona)
        datos_razones.append(valor)
    
    if sum(datos_razones) > 0:
        # Filtrar datos y nombres para valores > 0
        datos_filtrados = []
        nombres_filtrados = []
        
        for dato, nombre in zip(datos_razones, nombres_cortos):
            if dato > 0:
                datos_filtrados.append(dato)
                nombres_filtrados.append(nombre)
        
        if len(datos_filtrados) > 0:
            fig, ax = plt.subplots(figsize=(12, 4))
            
            colores = plt.cm.viridis(np.linspace(0, 1, len(datos_filtrados)))
            
            left = 0
            for valor, nombre, color in zip(datos_filtrados, nombres_filtrados, colores):
                ax.barh(0, valor, left=left, color=color, height=0.6)
                # Mostrar porcentaje solo si hay espacio suficiente
                if valor > 5:
                    ax.text(left + valor/2, 0, f'{valor:.1f}%', 
                           ha='center', va='center', color='white', fontweight='bold', fontsize=10)
                left += valor
            
            ax.set_xlim(0, sum(datos_filtrados))
            ax.set_yticks([])
            ax.set_xlabel('Porcentaje (%)', fontsize=12)
            ax.set_title(f'Razones para Caminar - {zona_seleccionada}', 
                        fontsize=16, fontweight='bold')
            
            # Crear leyenda personalizada sin repetir información
            leyenda_items = [f'{nombre}: {valor:.1f}%' for nombre, valor in zip(nombres_filtrados, datos_filtrados)]
            ax.legend(leyenda_items, bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
            ax.grid(axis='x', alpha=0.3)
            
            plt.tight_layout()
        else:
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.text(0.5, 0.5, 'Sin datos válidos para razones', 
                   transform=ax.transAxes, ha='center', va='center', fontsize=14)
            ax.axis('off')
    else:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no disponibles para razones', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.axis('off')
    
    return fig

def grafico_radar(betas_df, zona_seleccionada, colores_dict):
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
    st.markdown(f"""Hallazgos basados en las encuestas realizadas en {titulo_ciudad}, 
    centrados en la percepción de caminabilidad y la calidad del espacio público.""")
    
    # Selector de zona
    zona_key = f"zona_{titulo_ciudad.lower().replace(' ', '_')}"
    zona_seleccionada = st.selectbox(f"Selecciona una zona de {titulo_ciudad}", zonas, key=zona_key)
    
    # Gráficos
    st.markdown("#### Distribución de Movilidad")
    st.pyplot(grafico_genero(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Tenencia de Vehículos")
    st.pyplot(grafico_vehiculos(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Medios de Transporte Utilizados")
    st.pyplot(grafico_transporte(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Razones para Caminar")
    st.pyplot(grafico_razones(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    # Perfil de caminabilidad
    st.subheader(f"Perfil de Caminabilidad - {titulo_ciudad}")
    st.markdown("Coeficientes que indican la relación entre variables y el índice de caminabilidad (rango: -2 a 2).")
    
    fig_radar = grafico_radar(betas_df, zona_seleccionada, colores_dict)
    st.plotly_chart(fig_radar, use_container_width=True)

def crear_graficos(hallazgos_vref, betas_GDL, betas_MDE):
    """Función principal que organiza toda la visualización"""
    
    colonias_guadalajara = ["Colinas", "Providencia", "Miramar"]
    colonias_medellin = ["Aguacatala", "Floresta", "Moravia"]
    
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

def main_hallazgos(visualizacion):
    """Función principal para el flujo de la aplicación"""
    if visualizacion == "Hallazgos":
        betas_GDL, betas_MDE, hallazgos_vref = cargar_datos()
        
        if all([betas_GDL is not None, betas_MDE is not None, hallazgos_vref is not None]):
            crear_graficos(hallazgos_vref, betas_GDL, betas_MDE)
        else:
            st.error("No se pudieron cargar los datos necesarios para la visualización.")