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


# Configuración de estilo para matplotlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
import plotly.graph_objects as go

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
        # Columnas 2, 3, 4 para Guadalajara (Colinas, Providencia, Miramar)
        # Ajustado porque parece que el CSV tiene columnas extra
        return colonias_gdl.index(zona) + 2
    else:  # Medellín
        # Columnas 5, 6, 7 para Medellín (Aguacatala, Floresta, Moravia)
        return colonias_mde.index(zona) + 5

def obtener_valor_seguro(fila, indice_zona):
    """Obtiene un valor de manera segura manejando diferentes tipos de índices"""
    try:
        if fila is None:
            return 0
        
        # Verificar que el índice está dentro del rango
        if indice_zona >= len(fila):
            return 0
            
        # Intentar obtener el valor usando iloc con índice entero
        if isinstance(indice_zona, int):
            valor_raw = fila.iloc[indice_zona]
        else:
            # Si no es entero, convertir
            valor_raw = fila.iloc[int(indice_zona)]
            
        # Convertir a float de manera segura
        return convertir_a_float_seguro(pd.Series([valor_raw])).iloc[0]
        
    except (IndexError, ValueError, TypeError) as e:
        print(f"Error al obtener valor en índice {indice_zona}: {e}")
        return 0

def grafico_genero(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de dona doble con distribución por género y movilidad"""
    
    # Obtener índice de la zona seleccionada
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    # --- Datos para el anillo exterior (Género) ---
    fila_mujeres = extraer_fila_por_concepto(df, "% Mujeres")
    porcentaje_mujeres = obtener_valor_seguro(fila_mujeres, indice_zona)
    porcentaje_mujeres = max(0, min(100, porcentaje_mujeres))
    porcentaje_hombres = 100 - porcentaje_mujeres
    
    # --- Datos para el anillo interior (Movilidad) ---
    fila_caminata = extraer_fila_por_concepto(df, "Caminata")
    fila_auto_moto = extraer_fila_por_concepto(df, "% con auto/moto")
    
    # Obtener valores usando la función segura
    porcentaje_caminata = obtener_valor_seguro(fila_caminata, indice_zona)
    porcentaje_auto_moto = obtener_valor_seguro(fila_auto_moto, indice_zona)
    
    # Calcular complementos
    porcentaje_no_caminata = max(0, 100 - porcentaje_caminata)
    porcentaje_no_auto_moto = max(0, 100 - porcentaje_auto_moto)
    
    # Verificar si hay datos válidos
    datos_validos = (porcentaje_mujeres + porcentaje_hombres > 0) or (porcentaje_caminata + porcentaje_auto_moto > 0)
    
    if not datos_validos:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, 'Datos no válidos para esta zona', 
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Distribución por Género y Movilidad - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
        return fig
    
    # --- Crear el gráfico de dona doble ---
    fig, ax = plt.subplots(figsize=(10, 10))
    
    # Colores
    colores_genero = ['#E91E63', '#2196F3']  # Rosa: Mujeres, Azul: Hombres
    colores_movilidad = ['#4CAF50', '#FFC107']  # Verde: Camina, Amarillo: Auto/Moto
    
    # Grosor de los anillos
    grosor_anillo = 0.3
    
    # 1. Anillo exterior (Género)
    if porcentaje_mujeres + porcentaje_hombres > 0:
        ax.pie(
            [porcentaje_mujeres, porcentaje_hombres],
            radius=1,
            colors=colores_genero,
            labels=['Mujeres', 'Hombres'],
            labeldistance=1.1,
            wedgeprops=dict(width=grosor_anillo, edgecolor='w'),
            autopct=lambda p: f'{p:.1f}%' if p > 5 else '',
            pctdistance=0.85,
            textprops={'fontsize': 12, 'fontweight': 'bold'},
            startangle=90
        )
    
    # 2. Anillo interior (Movilidad)
    if porcentaje_caminata + porcentaje_auto_moto > 0:
        ax.pie(
            [porcentaje_caminata, porcentaje_no_caminata, porcentaje_auto_moto, porcentaje_no_auto_moto],
            radius=1-grosor_anillo-0.05,  # Un poco más pequeño que el exterior
            colors=['#4CAF50', '#F44336', '#FFC107', '#9E9E9E'],
            labels=['Camina', 'No camina', 'Tiene auto/moto', 'No tiene'],
            labeldistance=0.75,
            wedgeprops=dict(width=grosor_anillo, edgecolor='w'),
            autopct=lambda p: f'{p:.1f}%' if p > 5 else '',
            pctdistance=0.7,
            textprops={'fontsize': 10, 'fontweight': 'bold'},
            startangle=90
        )
    
    # Añadir título y leyenda
    ax.set_title(f'Distribución por Género y Movilidad\n{zona_seleccionada}', 
                fontsize=18, fontweight='bold', pad=30)
    
    # Añadir un círculo blanco en el centro para mejor legibilidad
    centro_circulo = plt.Circle((0, 0), 0.4, color='white')
    ax.add_artist(centro_circulo)
    
    # Añadir texto explicativo en el centro
    texto_centro = ""
    if porcentaje_caminata > 0:
        texto_centro += f"Camina: {porcentaje_caminata:.1f}%\n"
    if porcentaje_auto_moto > 0:
        texto_centro += f"Auto/Moto: {porcentaje_auto_moto:.1f}%"
    
    ax.text(0, 0, texto_centro, ha='center', va='center', 
           fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    return fig

def grafico_vehiculos(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de dona de tenencia de vehículos para la zona seleccionada"""
    
    # Extraer datos de vehículos
    fila_auto = extraer_fila_por_concepto(df, "% con auto")
    fila_auto_moto = extraer_fila_por_concepto(df, "% con auto/moto")
    
    if fila_auto is not None and fila_auto_moto is not None:
        # Obtener índice de la zona seleccionada
        indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
        
        # Obtener porcentajes para la zona seleccionada usando la función segura
        porcentaje_auto = obtener_valor_seguro(fila_auto, indice_zona)
        porcentaje_auto_moto = obtener_valor_seguro(fila_auto_moto, indice_zona)
        
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

def grafico_transporte(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de barras horizontales de medios de transporte para la zona seleccionada"""
    
    # Extraer datos de medios de transporte
    medios = ['Caminata', 'Auto', 'Bus / SITVA', 'Motocicleta']
    datos_transporte = []
    
    # Obtener índice de la zona seleccionada
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    for medio in medios:
        fila = extraer_fila_por_concepto(df, medio)
        valor = obtener_valor_seguro(fila, indice_zona)
        datos_transporte.append(valor)
    
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

def grafico_razones(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de barra horizontal apilada de razones para caminar para la zona seleccionada"""
    
    # Razones para caminar
    razones = ['Cercanía al lugar', 'Por salud', 'Única alternativa', 
               'Distracción /desestrés/ Gusto /Apreciación', 'Ahorrar tiempo', 'No tengo carro']
    nombres_cortos = ['Cercanía', 'Salud', 'Única alternativa', 'Gusto', 'Ahorrar tiempo', 'No tengo carro']
    
    datos_razones = []
    
    # Obtener índice de la zona seleccionada
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)
    
    for razon in razones:
        fila = extraer_fila_por_concepto(df, razon)
        valor = obtener_valor_seguro(fila, indice_zona)
        datos_razones.append(valor)
    
    if sum(datos_razones) > 0:
        # Crear gráfico de barra apilada horizontal
        fig, ax = plt.subplots(figsize=(12, 4))
        
        # Usar solo los nombres cortos que corresponden a datos positivos
        nombres_usados = [n for n, d in zip(nombres_cortos, datos_razones) if d > 0]
        datos_usados = [d for d in datos_razones if d > 0]
        
        # Crear colores
        colores = plt.cm.viridis(np.linspace(0, 1, len(datos_usados)))
        
        # Barra apilada horizontal
        left = 0
        for valor, nombre, color in zip(datos_usados, nombres_usados, colores):
            ax.barh(0, valor, left=left, color=color, label=f'{nombre}: {valor:.1f}%', height=0.6)
            # Mostrar el valor dentro de cada segmento
            ax.text(left + valor/2, 0, f'{valor:.1f}%', 
                   ha='center', va='center', color='white', fontweight='bold')
            left += valor
        
        ax.set_xlim(0, 100)
        ax.set_yticks([])
        ax.set_xlabel('Porcentaje (%)', fontsize=12)
        ax.set_title(f'Razones para Caminar - {zona_seleccionada}', 
                    fontsize=16, fontweight='bold')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
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
    st.markdown(f"""En esta sección se presentan los hallazgos más relevantes de la investigación, 
    basados en las encuestas realizadas para ciudad de {titulo_ciudad}. Estos hallazgos se centran 
    en la percepción de caminabilidad y la calidad del espacio público en diferentes colonias de la ciudad.""")
    
    # Selector de zona para toda la ciudad
    zona_key = f"zona_{titulo_ciudad.lower().replace(' ', '_')}"
    zona_seleccionada = st.selectbox(f"Selecciona una zona de {titulo_ciudad}", zonas, key=zona_key)
    
    # Gráficos demográficos y de comportamiento para la zona seleccionada
    st.markdown("#### Distribución por Género")
    st.pyplot(grafico_genero(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Tenencia de Vehículos")
    st.pyplot(grafico_vehiculos(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Medios de Transporte Utilizados")
    st.pyplot(grafico_transporte(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    st.markdown("#### Razones para Caminar")
    st.pyplot(grafico_razones(df, zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad))
    
    # Sección de betas - solo gráfico radar
    st.subheader(f"Perfil de Caminabilidad - {titulo_ciudad}")
    st.markdown("Los betas son coeficientes que indican la relación entre las variables y el índice de caminabilidad. Van de -2 a 2, donde valores negativos indican una relación inversa y positivos una relación directa.")
    
    # Gráfico radar (usa la misma zona seleccionada)
    fig_radar = grafico_radar(betas_df, zona_seleccionada, colores_dict)
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
def main_hallazgos(visualizacion):
    if visualizacion == "Hallazgos":
        # Cargar datos con cache
        betas_GDL, betas_MDE, hallazgos_vref = cargar_datos()
        
        if all([betas_GDL is not None, betas_MDE is not None, hallazgos_vref is not None]):
            crear_graficos(hallazgos_vref, betas_GDL, betas_MDE)
        else:
            st.error("No se pudieron cargar los datos necesarios para la visualización.")