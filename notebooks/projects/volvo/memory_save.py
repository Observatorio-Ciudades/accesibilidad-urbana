import os
import streamlit as st
import geopandas as gpd

@st.cache_data(ttl=3600)
def cargar_poligonos_estudio(base_path, ciudad):
    """Carga y simplifica los polígonos de estudio solo para la ciudad seleccionada"""
    if not ciudad:
        return None
        
    try:
        archivo = "PolígonosEstudio.gpkg" if ciudad == 'Guadalajara' else "PoligonoEstudio_MDE.gpkg"
        ruta = os.path.join(base_path, ciudad, archivo)  # Añadida carpeta por ciudad
        
        if not os.path.exists(ruta):
            st.warning(f"Archivo no encontrado para {ciudad}: {ruta}")
            return None
            
        gdf = gpd.read_file(ruta)
        if gdf.empty:
            st.warning(f"No hay datos disponibles para {ciudad}")
            return None
            
        # Simplificación geométrica para mejorar rendimiento
        gdf['geometry'] = gdf.geometry.simplify(tolerance=0.0005, preserve_topology=True)
        
        st.success(f"Polígonos cargados exitosamente para {ciudad}")
        return gdf
    
    except Exception as e:
        st.error(f"Error cargando polígonos para {ciudad}: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def cargar_variables_fisicas(ruta_base, ciudad):
    """Carga variables físicas solo para la ciudad seleccionada"""
    if not ciudad:
        return {}
        
    try:
        variables = {}
        ruta_ciudad = os.path.join(ruta_base, ciudad)
        
        if not os.path.exists(ruta_ciudad):
            st.warning(f"Carpeta de datos no encontrada para {ciudad}: {ruta_ciudad}")
            return variables
            
        archivos_encontrados = []
        for archivo in os.listdir(ruta_ciudad):
            if archivo.endswith(('.shp', '.gpkg')):
                archivos_encontrados.append(archivo)
                
        if not archivos_encontrados:
            st.warning(f"No se encontraron archivos de variables físicas para {ciudad}")
            return variables
            
        # Crear barra de progreso para mostrar el progreso de carga
        progress_bar = st.progress(0)
        total_archivos = len(archivos_encontrados)
        
        for i, archivo in enumerate(archivos_encontrados):
            ruta = os.path.join(ruta_ciudad, archivo)
            try:
                # Limitar datos para optimizar rendimiento
                gdf = gpd.read_file(ruta, rows=1000)
                if not gdf.empty and 'geometry' in gdf.columns:
                    if gdf.crs != 'EPSG:4326':
                        gdf = gdf.to_crs('EPSG:4326')
                    variables[archivo] = gdf
                    
                # Actualizar barra de progreso
                progress_bar.progress((i + 1) / total_archivos)
                
            except Exception as e:
                st.warning(f"Error cargando {archivo} para {ciudad}: {str(e)}")
                continue
                
        progress_bar.empty()  # Limpiar barra de progreso
        
        if variables:
            st.success(f"Variables físicas cargadas para {ciudad}: {len(variables)} archivos")
        else:
            st.warning(f"No se pudieron cargar variables físicas para {ciudad}")
            
        return variables
        
    except Exception as e:
        st.error(f"Error cargando variables físicas para {ciudad}: {str(e)}")
        return {}

@st.cache_data(ttl=3600)
def cargar_datos_escuelas(ciudad=None):
    """Carga datos de escuelas, opcionalmente filtrados por ciudad"""
    if not ciudad:
        st.warning("No se ha seleccionado una ciudad para cargar datos de escuelas")
        return None, None
        
    try:
        # Ruta específica por ciudad si existe
        ruta_base = "../../WalkabilityIndex" # Poner la ruta base correcta
        if not os.path.exists(ruta_base):
            st.warning("Ruta base de datos no encontrada")
            return None, None
        ruta = os.path.join(ruta_base, ciudad, "volvo_wgtproxanalysis_2024_mza_hex9.geojson")
        
        # Si no existe ruta específica por ciudad, usar ruta general
        if not os.path.exists(ruta):
            ruta = os.path.join(ruta_base, "volvo_wgtproxanalysis_2024_mza_hex9.geojson")
            
        if not os.path.exists(ruta):
            st.warning(f"Archivo de datos de escuelas no encontrado para {ciudad}")
            return None, None
            
        columnas = ['max_escuelas', 'min_escuelas']
        
        # Cargar solo las columnas necesarias
        gdf = gpd.read_file(ruta, columns=columnas + ['geometry'])
        
        if not all(col in gdf.columns for col in columnas):
            st.warning(f"Columnas requeridas no encontradas en datos de {ciudad}")
            return None, None
        
        # Filtrar por geometría si es necesario (implementar filtro espacial aquí)
        # gdf = filtrar_por_ciudad_geometria(gdf, ciudad)
            
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
        
        st.success(f"Datos de escuelas cargados para {ciudad}")
        return max_stats, min_stats
        
    except Exception as e:
        st.error(f"Error cargando datos de escuelas para {ciudad}: {str(e)}")
        return None, None

@st.cache_data(ttl=3600)
def cargar_edges_sector(base_path, ciudad, sector=None, simplificar=True):
    """Carga datos de edges solo para la ciudad y sector específicos"""
    if not ciudad:
        st.warning("No se ha seleccionado una ciudad")
        return None
        
    try:
        # Construir ruta específica por ciudad
        if sector:
            sector_path = os.path.join(base_path, ciudad, sector)
        else:
            sector_path = os.path.join(base_path, ciudad)
            
        if not os.path.exists(sector_path):
            st.warning(f"Ruta no encontrada para {ciudad}" + (f" sector {sector}" if sector else ""))
            return None
            
        archivos = [f for f in os.listdir(sector_path) if f.endswith('edges_proj_net_final.shp')]
        if not archivos:
            st.warning(f"No se encontraron archivos de edges para {ciudad}" + (f" sector {sector}" if sector else ""))
            return None
            
        # Cargar solo el primer archivo encontrado con límite de filas
        ruta_archivo = os.path.join(sector_path, archivos[0])
        gdf = gpd.read_file(ruta_archivo, rows=500)
        
        if gdf.empty:
            st.warning(f"Archivo de edges vacío para {ciudad}")
            return None
            
        # Conversión CRS específica por ciudad
        target_crs = 'EPSG:32613' if ciudad == 'Guadalajara' else 'EPSG:32618' # Ajustar dependiendo de los datos.
        
        if gdf.crs != target_crs:
            gdf = gdf.to_crs(target_crs)
        
        if simplificar:
            gdf['geometry'] = gdf.geometry.simplify(tolerance=1)
            
        st.success(f"Edges cargados para {ciudad}" + (f" sector {sector}" if sector else ""))
        return gdf
        
    except Exception as e:
        st.error(f"Error cargando edges para {ciudad}: {str(e)}")
        return None

def limpiar_cache_ciudad(ciudad_anterior):
    """Limpia el caché de datos de la ciudad anterior para liberar memoria"""
    try:
        # Limpiar caché específico de streamlit
        st.cache_data.clear()
        # st.success(f"Caché limpiado para {ciudad_anterior}")
    except Exception as e:
        st.warning(f"Error limpiando caché: {str(e)}")

# Función auxiliar para gestión de datos por ciudad
def inicializar_datos_ciudad(ciudad, rutas_config):
    """Inicializa todos los datos necesarios para una ciudad específica"""
    if not ciudad:
        st.warning("Selecciona una ciudad para cargar los datos")
        return None
    
    st.info(f"Cargando datos para {ciudad}...")
    
    datos_ciudad = {
        'ciudad': ciudad,
        'poligonos': None,
        'variables_fisicas': {},
        'escuelas_stats': (None, None),
        'edges': None
    }
    
    # Cargar datos solo si se proporciona la configuración de rutas
    if 'poligonos_path' in rutas_config:
        datos_ciudad['poligonos'] = cargar_poligonos_estudio(
            rutas_config['poligonos_path'], ciudad
        )
    
    if 'variables_path' in rutas_config:
        datos_ciudad['variables_fisicas'] = cargar_variables_fisicas(
            rutas_config['variables_path'], ciudad
        )
    
    if 'cargar_escuelas' in rutas_config and rutas_config['cargar_escuelas']:
        datos_ciudad['escuelas_stats'] = cargar_datos_escuelas(ciudad)
    
    if 'edges_path' in rutas_config:
        datos_ciudad['edges'] = cargar_edges_sector(
            rutas_config['edges_path'], ciudad
        )
    
    return datos_ciudad
