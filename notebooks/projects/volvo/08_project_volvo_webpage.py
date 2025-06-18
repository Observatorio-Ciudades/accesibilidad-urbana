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

# Datos a utilizar:
data_caminabilidad_GDL = pd.read_excel("/home/jovyan/accesibilidad-urbana/data/external/Encuesta_Caminata_GDL.xlsx")
# Falta la base de datos para Medellín

# Configuración específica para cada tipo de visualización
if visualizacion == "Mapas":
    # Para mapas: selección individual de ciudad
    ciudad_seleccionada = st.selectbox(
        "Seleccione la ciudad que desea visualizar:", 
        ('Guadalajara', 'Medellín'), 
        key='ciudad_mapas'
    )
    
    # Función para cargar polígonos de estudio
    def cargar_poligonos_estudio(ruta_archivo):
        """
        Carga el archivo PoligonosEstudio.gpkg y lo añade al mapa
        """
        try:
            if os.path.exists(ruta_archivo):
                gdf_poligonos = gpd.read_file(ruta_archivo)
                if 'geometry' not in gdf_poligonos.columns or gdf_poligonos.empty:
                    return None
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

        # Cargar polígonos de estudio
        ruta_poligonos = os.path.join(base_path, "PolígonosEstudio.gpkg")
        gdf_poligonos = cargar_poligonos_estudio(ruta_poligonos)
        
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
            
            # HQSL Choropleth legend
            st.markdown("""
                <div style="text-align: center; font-size: 12px; font-weight: bold; margin-bottom: 10px;">
                    Calidad de Vida Social (HQSL)
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
                    <div style="margin-left: 10px; font-size: 12px;">
                        <div>Mayor</div>
                        <div style="margin-top: 20px;">Menor</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Calidad del espacio público legend
            st.markdown("""
                <div style="text-align: center; font-size: 12px; font-weight: bold; margin-bottom: 10px;">
                    Calidad del espacio público para la movilidad activa
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
                    <div style="margin-left: 10px; font-size: 12px;">
                        <div>Mayor</div>
                        <div style="margin-top: 20px;">Menor</div>
                    </div>
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

elif visualizacion == "Hallazgos":
    # Para hallazgos: layout de dos columnas
    col1, col2 = st.columns(2)

    # Contenido para la columna de Guadalajara
    with col1:
        st.header("Guadalajara")
        
        # ==== Análisis de datos Guadalajara ====
        # Gráfica de pastel según el género
        # Obtener datos cruzados
        cross_tab = pd.crosstab(
            data_caminabilidad_GDL['¿Hace uso de algún equipamiento/sitio de interés localizado en el sector/territorio?'],
            data_caminabilidad_GDL['Género']
        )

        # Definir colores personalizados
        outer_colors = ['#FF6B6B', '#4ECDC4']  # Colores para equipamiento (No/Sí)
        inner_colors = ['#FFA07A', '#FFD166', '#7FB3D5', '#A2D9CE']  # Colores para géneros

        st.subheader("¿Hace uso de algún equipamiento/sitio de interés localizado en el sector/territorio?")

        fig, ax = plt.subplots(figsize=(12, 12)) 

        # --- Anillo Exterior (Equipamiento) ---
        outer_wedges, outer_texts, outer_autotexts = ax.pie(
            cross_tab.sum(axis=1),
            labels=cross_tab.index,
            autopct='%1.1f%%',
            startangle=90,
            pctdistance=0.85,
            wedgeprops=dict(width=0.4, edgecolor='w', linewidth=2),
            colors=outer_colors,
            textprops={'fontsize': 12, 'weight': 'bold'}
        )

        # --- Anillo Interior (Género) ---
        # Crear los valores y etiquetas
        inner_values = []
        inner_labels = []
        inner_color_map = []

        # Iterar por cada categoría de equipamiento y género
        color_idx = 0
        for i, equip_category in enumerate(cross_tab.index):
            for j, gender in enumerate(cross_tab.columns):
                value = cross_tab.loc[equip_category, gender]
                inner_values.append(value)
                inner_labels.append(f"{gender}\n({value})")
                inner_color_map.append(inner_colors[color_idx])
                color_idx += 1

        # Crear el anillo interior
        inner_wedges, inner_texts = ax.pie(
            inner_values,
            labels=inner_labels,
            radius=0.6,
            wedgeprops=dict(width=0.3, edgecolor='w', linewidth=2),
            startangle=90,
            colors=inner_color_map,
            labeldistance=0.4,  # Colocar etiquetas más cerca del centro
            textprops={'fontsize': 9, 'ha': 'center', 'va': 'center', 'weight': 'bold'}
        )

        # --- Leyenda Mejorada ---
        legend_elements = [
            # Categorías principales
            Patch(facecolor=outer_colors[0], label=f'No usa equipamiento ({cross_tab.sum(axis=1).iloc[0]} personas)'),
            Patch(facecolor=outer_colors[1], label=f'Sí usa equipamiento ({cross_tab.sum(axis=1).iloc[1]} personas)'),
            # Separador
            Patch(facecolor='white', label=''),
            # Subcategorías por género
        ]

        # Agregar las subcategorías dinámicamente
        color_idx = 0
        for i, equip_category in enumerate(cross_tab.index):
            for j, gender in enumerate(cross_tab.columns):
                value = cross_tab.loc[equip_category, gender]
                status = "No usan" if equip_category == cross_tab.index[0] else "Sí usan"
                legend_elements.append(
                    Patch(facecolor=inner_colors[color_idx], 
                        label=f'{gender} - {status} ({value})')
                )
                color_idx += 1

        ax.legend(
            handles=legend_elements,
            title="Distribución por Equipamiento y Género",
            loc="upper center",
            bbox_to_anchor=(0.5, -0.02),
            frameon=True,
            fancybox=True,
            shadow=True,
            title_fontsize=16,
            fontsize=14,
            ncol=2  
        )

        # Ajustar diseño con más espacio para la leyenda inferior
        plt.subplots_adjust(bottom=0.1)  # Dejar espacio en la parte inferior
        ax.axis('equal')

        st.pyplot(fig)

        # ===Gráfica para el tiempo promedio caminado===
        
        st.subheader("¿Cuánto tiempo le tomó el recorrido caminando para legar a su lugar de destino?")
        # Crear un histograma del tiempo promedio caminado
        tiempo_caminado = data_caminabilidad_GDL['¿Cuánto tiempo le tomó el recorrido caminando para legar a su lugar de destino? (Ingresar en minutos)'].dropna()
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.hist(tiempo_caminado, bins=30, color='#4ECDC4', edgecolor='black')
        ax.set_xlabel("Tiempo (minutos)", fontsize=14)
        ax.set_ylabel("Frecuencia", fontsize=14)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        st.pyplot(fig)
        
        # === Histogramas de importancia de la caminata ===

        categorias = st.selectbox('Selecciona una categoría:', 
                                ['Seguridad', 'Mixtura del suelo', 'Intersecciones', 'Vegetación', 
                                'Densidad de población', 'Presencia de Andares', 'Proximidad'], key='categorias_gdl')

        # Función para convertir texto a escala numérica
        def convertir_a_escala_numerica(datos_texto):
            """
            Convierte las respuestas de texto a escala numérica -2 a 2
            con manejo robusto de diferentes formatos de texto
            """
            mapeo_escala = {
                "no es importante": -2,
                "es poco importante": -1,
                "moderadamente importante": 0,
                "es importante": 1,
                "muy importante": 2,
                # Variaciones comunes que podrían aparecer
                "importante": 1,
                "poco importante": -1,
                "no importante": -2
            }
            
            # Convertir a minúsculas y eliminar espacios extras
            datos_limpios = datos_texto.str.lower().str.strip()
            
            # Mapear y convertir a numérico
            datos_numericos = datos_limpios.map(mapeo_escala)
            
            # Eliminar valores no mapeados (NaN)
            datos_numericos = datos_numericos.dropna()
            
            return datos_numericos

        # Función para crear histograma con escala de importancia
        def crear_histograma_importancia(datos_texto, titulo, color, xlabel):
            """
            Crea un histograma con escala de importancia de -2 a 2
            Convierte automáticamente de texto a números
            """
            
            datos_numericos = convertir_a_escala_numerica(datos_texto)

            if len(datos_numericos) == 0:
                st.warning("No hay datos válidos para mostrar en esta categoría.")
                return None

            fig, ax = plt.subplots(figsize=(12, 10))

            # Añadir información sobre la muestra
            total_respuestas = len(datos_numericos)
            ax.text(0.02, 0.98, f'Total de respuestas válidas: {total_respuestas}', 
                    transform=ax.transAxes, fontsize=11, 
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

            # Crear bins específicos para la escala -2 a 2
            bins = [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]

            # Crear el histograma con datos numéricos
            counts, _, patches = ax.hist(datos_numericos, bins=bins, color=color, edgecolor='black', alpha=0.7)

            # Personalizar los ticks del eje X
            ax.set_xticks([-2, -1, 0, 1, 2])
            ax.set_xticklabels([
                'No es\nimportante\n(-2)', 
                'Poco\nimportante\n(-1)', 
                'Moderadamente\nimportante\n(0)', 
                'Es\nimportante\n(1)', 
                'Muy\nimportante\n(2)'
            ], fontsize=10)
            
            # Configurar límites del eje X
            ax.set_xlim(-2.7, 2.7)
            
            # Etiquetas y título
            ax.set_xlabel(xlabel, fontsize=14, fontweight='bold')
            ax.set_ylabel("Frecuencia", fontsize=14, fontweight='bold')
            ax.set_title(titulo, fontsize=16, fontweight='bold', pad=20)
            
            # Grid mejorado
            ax.grid(axis='y', linestyle='--', alpha=0.5)
            ax.set_axisbelow(True)
            
            # Añadir valores encima de cada barra
            for i, count in enumerate(counts):
                if count > 0:  # Solo mostrar si hay datos
                    ax.text([-2, -1, 0, 1, 2][i], count + max(counts)*0.01, 
                        f'{int(count)}', ha='center', va='bottom', fontweight='bold')
            
            # Estadísticas descriptivas (solo si hay datos)
            if len(datos_numericos) > 0:
                media = datos_numericos.mean()
                mediana = datos_numericos.median()
                moda = datos_numericos.mode().iloc[0]
                
                # Añadir líneas de referencia
                #ax.axvline(media, color='red', linestyle='--', alpha=0.8, linewidth=2, label=f'Media: {media:.2f}')
                ax.axvline(mediana, color='maroon', linestyle='--', alpha=0.8, linewidth=4, label=f'Mediana: {mediana:.2f}')
                ax.axvline(moda, color='darkgreen', linestyle='--', alpha=0.8, linewidth=4, label=f'Moda: {moda:.2f}')
                
                # Leyenda
                ax.legend(loc='upper right', framealpha=0.9)
            
            plt.tight_layout()
            
            return fig

        # Procesamiento según la categoría seleccionada para Guadalajara
        if categorias == 'Seguridad':
            st.subheader("📍 Importancia de la Seguridad")
            datos_texto = data_caminabilidad_GDL['Sentirme seguro/a frente a posibles delitos en mi trayecto'].dropna()
            fig = crear_histograma_importancia(
                datos_texto, 
                "Distribución de Importancia: Seguridad en el Trayecto",
                '#FF6B6B',
                "Nivel de Importancia de la Seguridad"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias == 'Mixtura del suelo':
            st.subheader("🏢 Importancia de la Mixtura del Suelo")
            datos_texto = data_caminabilidad_GDL['La posibilidad de hacer múltiples vueltas/trámites en mi recorrido'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Mixtura del Suelo",
                '#4ECDC4',
                "Nivel de Importancia de la Mixtura del Suelo"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias == 'Intersecciones':        
            st.subheader("🛣️ Importancia de las Intersecciones")
            datos_texto = data_caminabilidad_GDL['La posibilidad de poder tomar desvíos y hacer múltiples trayectos en mi recorrido'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Intersecciones y Desvíos",
                '#FFD166',
                "Nivel de Importancia de las Intersecciones"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias == 'Vegetación':
            st.subheader("🌳 Importancia de la Vegetación")
            
            # Primer gráfico: Sombra de vegetación
            st.write("**Comodidad por sombra de vegetación:**")
            datos_texto1 = data_caminabilidad_GDL['Sentirme cómodo/a por la sombra generada por la vegetación'].dropna()
            fig1 = crear_histograma_importancia(
                datos_texto1,
                "Distribución de Importancia: Sombra de Vegetación",
                '#7FB3D5',
                "Nivel de Importancia de la Sombra"
            )
            if fig1:
                st.pyplot(fig1)
            
            # Segundo gráfico: Paisaje agradable
            st.write("**Paisaje agradable:**")
            datos_texto2 = data_caminabilidad_GDL['Que el paisaje sea agradable'].dropna()
            fig2 = crear_histograma_importancia(
                datos_texto2,
                "Distribución de Importancia: Paisaje Agradable",
                '#98D8C8',
                "Nivel de Importancia del Paisaje"
            )
            if fig2:
                st.pyplot(fig2)
            
        elif categorias == 'Densidad de población':
            st.subheader("👥 Importancia de la Densidad de Población")
            datos_texto = data_caminabilidad_GDL['Sentirme cómodo/a en términos de la cantidad de gente a lo largo de mi recorrido'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Densidad de Población",
                '#A2D9CE',
                "Nivel de Importancia de la Densidad Poblacional"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias == 'Presencia de Andares':
            st.subheader("🚶 Importancia de la Presencia de Andenes")
            datos_texto = data_caminabilidad_GDL['La presencia de andenes'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Presencia de Andenes",
                '#FF6B6B',
                "Nivel de Importancia de los Andenes"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias == 'Proximidad':
            st.subheader("📍 Importancia de la Proximidad")
            datos_texto = data_caminabilidad_GDL['La cercanía del equipamiento X'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Proximidad al Equipamiento",
                '#E74C3C',
                "Nivel de Importancia de la Proximidad"
            )
            if fig:
                st.pyplot(fig)

    # Contenido para la columna de Medellín
    with col2:
        st.header("Medellín")
        
        # Nota: Aquí usarías los datos de Medellín cuando estén disponibles
        # Por ahora, uso los mismos datos de Guadalajara como placeholder
        
        # Gráfica de pastel según el género (usando datos de GDL como placeholder)
        cross_tab_med = pd.crosstab(
            data_caminabilidad_GDL['¿Hace uso de algún equipamiento/sitio de interés localizado en el sector/territorio?'],
            data_caminabilidad_GDL['Género']
        )

        # Definir colores personalizados
        outer_colors = ['#FF6B6B', '#4ECDC4']  # Colores para equipamiento (No/Sí)
        inner_colors = ['#FFA07A', '#FFD166', '#7FB3D5', '#A2D9CE']  # Colores para géneros

        st.subheader("¿Hace uso de algún equipamiento/sitio de interés localizado en el sector/territorio?")

        fig, ax = plt.subplots(figsize=(12, 12)) 

        # --- Anillo Exterior (Equipamiento) ---
        outer_wedges, outer_texts, outer_autotexts = ax.pie(
            cross_tab_med.sum(axis=1),
            labels=cross_tab_med.index,
            autopct='%1.1f%%',
            startangle=90,
            pctdistance=0.85,
            wedgeprops=dict(width=0.4, edgecolor='w', linewidth=2),
            colors=outer_colors,
            textprops={'fontsize': 12, 'weight': 'bold'}
        )

        # --- Anillo Interior (Género) ---
        inner_values = []
        inner_labels = []
        inner_color_map = []

        color_idx = 0
        for i, equip_category in enumerate(cross_tab_med.index):
            for j, gender in enumerate(cross_tab_med.columns):
                value = cross_tab_med.loc[equip_category, gender]
                inner_values.append(value)
                inner_labels.append(f"{gender}\n({value})")
                inner_color_map.append(inner_colors[color_idx])
                color_idx += 1

        inner_wedges, inner_texts = ax.pie(
            inner_values,
            labels=inner_labels,
            radius=0.6,
            wedgeprops=dict(width=0.3, edgecolor='w', linewidth=2),
            startangle=90,
            colors=inner_color_map,
            labeldistance=0.4,
            textprops={'fontsize': 9, 'ha': 'center', 'va': 'center', 'weight': 'bold'}
        )

        # --- Leyenda Mejorada ---
        legend_elements = [
            Patch(facecolor=outer_colors[0], label=f'No usa equipamiento ({cross_tab_med.sum(axis=1).iloc[0]} personas)'),
            Patch(facecolor=outer_colors[1], label=f'Sí usa equipamiento ({cross_tab_med.sum(axis=1).iloc[1]} personas)'),
            Patch(facecolor='white', label=''),
        ]

        color_idx = 0
        for i, equip_category in enumerate(cross_tab_med.index):
            for j, gender in enumerate(cross_tab_med.columns):
                value = cross_tab_med.loc[equip_category, gender]
                status = "No usan" if equip_category == cross_tab_med.index[0] else "Sí usan"
                legend_elements.append(
                    Patch(facecolor=inner_colors[color_idx], 
                        label=f'{gender} - {status} ({value})')
                )
                color_idx += 1

        ax.legend(
            handles=legend_elements,
            title="Distribución por Equipamiento y Género",
            loc="upper center",
            bbox_to_anchor=(0.5, -0.02),
            frameon=True,
            fancybox=True,
            shadow=True,
            title_fontsize=16,
            fontsize=14,
            ncol=2  
        )

        plt.subplots_adjust(bottom=0.1)
        ax.axis('equal')
        st.pyplot(fig)

        # ===Gráfica para el tiempo promedio caminado===
        st.subheader("¿Cuánto tiempo le tomó el recorrido caminando para legar a su lugar de destino?")
        tiempo_caminado_med = data_caminabilidad_GDL['¿Cuánto tiempo le tomó el recorrido caminando para legar a su lugar de destino? (Ingresar en minutos)'].dropna()
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.hist(tiempo_caminado_med, bins=30, color='#4ECDC4', edgecolor='black')
        ax.set_xlabel("Tiempo (minutos)", fontsize=14)
        ax.set_ylabel("Frecuencia", fontsize=14)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        st.pyplot(fig)
        
        # === Histogramas de importancia de la caminata para Medellín ===
        categorias_Medellin = st.selectbox(
            'Selecciona una categoría:', 
            ['Seguridad', 'Mixtura del suelo', 'Intersecciones', 'Vegetación', 
            'Densidad de población', 'Presencia de Andares', 'Proximidad'],
            key='categorias_medellin'
        )

        # Reutilizar las mismas funciones definidas anteriormente
        # Procesamiento según la categoría seleccionada para Medellín
        if categorias_Medellin == 'Seguridad':
            st.subheader("📍 Importancia de la Seguridad")
            datos_texto = data_caminabilidad_GDL['Sentirme seguro/a frente a posibles delitos en mi trayecto'].dropna()
            fig = crear_histograma_importancia(
                datos_texto, 
                "Distribución de Importancia: Seguridad en el Trayecto",
                '#FF6B6B',
                "Nivel de Importancia de la Seguridad"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias_Medellin == 'Mixtura del suelo':
            st.subheader("🏢 Importancia de la Mixtura del Suelo")
            datos_texto = data_caminabilidad_GDL['La posibilidad de hacer múltiples vueltas/trámites en mi recorrido'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Mixtura del Suelo",
                '#4ECDC4',
                "Nivel de Importancia de la Mixtura del Suelo"
            )
            if fig:
                st.pyplot(fig)
            
        elif categorias_Medellin == 'Intersecciones':
            st.subheader("🛣️ Importancia de las Intersecciones")
            datos_texto = data_caminabilidad_GDL['La posibilidad de poder tomar desvíos y hacer múltiples trayectos en mi recorrido'].dropna()
            fig = crear_histograma_importancia(
                datos_texto,
                "Distribución de Importancia: Intersecciones y Desvíos",
                '#FFD166',
                "Nivel de Importancia de las Intersecciones"
            )
            if fig:
                st.pyplot(fig)
        