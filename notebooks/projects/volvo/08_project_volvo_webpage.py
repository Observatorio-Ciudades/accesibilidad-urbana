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

# Selectbox para tipos de territorios de análisis
calidad_de_vida = st.selectbox("Seleccione el tipo calidad que quiera ver", 
                               ('Alto-bajo', 'Bajo-alto', 'Alto-alto'), key ='calidad_de_vida')

# División en dos columnas
col1, col2 = st.columns(2)

# Contenido para la columna de Guadalajara
with col1:
    st.header("Guadalajara")
    if visualizacion == "Mapas":

        def edges_to_map_all_sectors(base_path):
            """
            Carga archivos *_edges.shp de todos los sectores y los muestra en un mapa,
            cada uno con un color distinto y control de capas.
            """
            if not os.path.exists(base_path):
                st.error(f"La ruta {base_path} no existe.")
                return

            # Coordenadas centradas en Guadalajara (prueba)
            m = folium.Map(location=[20.6736, -103.344], zoom_start=13, tiles="cartodbpositron")

            # Paleta de colores para los sectores
            color_palette = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
                "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"
            ]

            # Mapeo fijo de sector a color
            sectores = sorted([d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))])
            colores = {sector: color_palette[i % len(color_palette)] for i, sector in enumerate(sectores)}

            for sector in sectores:
                sector_path = os.path.join(base_path, sector)

                for file in os.listdir(sector_path):
                    if file.endswith('_edges.shp') or file.endswith('_edges_proj_net_final.shp'):
                        file_path = os.path.join(sector_path, file)
                        try:
                            gdf = gpd.read_file(file_path)

                            tooltip_fields = [col for col in gdf.columns if isinstance(col, str)][:2]

                            folium.GeoJson(
                                gdf,
                                name=f"{sector} - {file}",
                                style_function=lambda feature, col=colores[sector]: {
                                    'color': col,
                                    'weight': 2,
                                    'opacity': 0.8
                                },
                                tooltip=folium.GeoJsonTooltip(fields=tooltip_fields)
                            ).add_to(m)

                        except Exception as e:
                            st.warning(f"No se pudo cargar {file} en {sector}: {e}")

            folium.LayerControl(collapsed=False).add_to(m)
            st.subheader("Mapa de líneas por sector (Guadalajara)")
            st_folium(m, width=900, height=400)

        # USO:
        st.sidebar.title("Opciones del mapa")
        ver_mapa = st.sidebar.checkbox("Mostrar mapas de líneas por sector", value=True)

        if ver_mapa:
            base_path = "/home/jovyan/accesibilidad-urbana/data/external/WalkabilityIndex/"
            edges_to_map_all_sectors(base_path)


    elif visualizacion == "Hallazgos":
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

        # ===Gráfica para el tiempo prmedio caminado===
        
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
                                'Densidad de población', 'Presencia de Andares', 'Proximidad'], key='categorias')

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

        # Procesamiento según la categoría seleccionada
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
    if visualizacion == "Mapas":
        # Crear un mapa centrado en Medellín
        mapa_med = folium.Map(location=[6.2442, -75.5812], zoom_start=12)
        folium.Marker([6.2442, -75.5812], tooltip="Medellín").add_to(mapa_med)
        st_folium(mapa_med, width=900, height=400)
    elif visualizacion == "Hallazgos":
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

        categorias_Medellin = st.selectbox(
            'Selecciona una categoría:', 
            ['Seguridad', 'Mixtura del suelo', 'Intersecciones', 'Vegetación', 
            'Densidad de población', 'Presencia de Andares', 'Proximidad'],
            key='selectbox_categorias_2'  # Key única para este selectbox
        )

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

        # Procesamiento según la categoría seleccionada
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
            
        elif categorias_Medellin == 'Vegetación':
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
            
        elif categorias_Medellin == 'Densidad de población':
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
            
        elif categorias_Medellin == 'Presencia de Andares':
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
            
        elif categorias_Medellin == 'Proximidad':
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
