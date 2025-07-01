import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import plotly.graph_objects as go
import plotly.express as px


def display_graphs_page():
    # Configuración de la página
    st.set_page_config(page_title="Proyecto Volvo", layout="wide")
    # Cargar datos con cache
    betas_GDL, betas_MDE, hallazgos_vref = load_data()

    # Check if all data is loaded successfully
    if all([betas_GDL is not None, betas_MDE is not None, hallazgos_vref is not None]):
        create_graphs(hallazgos_vref, betas_GDL, betas_MDE)



def create_graphs(hallazgos_vref, betas_GDL, betas_MDE):
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


@st.cache_data
def load_data():
    """Carga los datos necesarios para la visualización"""
    try:
        grl_dir = '../../../data/processed/vref/'
        betas_GDL = pd.read_excel(grl_dir+'BETAS_GDL_MDE.xlsx', sheet_name="GDL")
        betas_MDE = pd.read_excel(grl_dir+'BETAS_GDL_MDE.xlsx', sheet_name="Medellin")
        hallazgos_vref = pd.read_csv(grl_dir+'hallazgos_vref_limpio.csv', encoding='iso-8859-1')

        # Limpiar datos de caracteres invisibles
        hallazgos_vref = limpiar_datos(hallazgos_vref)

        return betas_GDL, betas_MDE, hallazgos_vref
    except Exception as e:
        st.error(f"Error while loading data: {e}")
        return None, None, None

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

    # Extraer datos de vehículos - buscar las filas correctas según el CSV
    fila_auto = extraer_fila_por_concepto(df, "Auto")  # Fila que dice "Auto"
    fila_auto_moto = extraer_fila_por_concepto(df, "% con auto/moto")  # Esta es la fila total

    # Obtener índice de la zona seleccionada
    indice_zona = obtener_indice_zona(zona_seleccionada, colonias_gdl, colonias_mde, titulo_ciudad)

    # Obtener porcentajes para la zona seleccionada
    porcentaje_solo_auto = obtener_valor_seguro(fila_auto, indice_zona) if fila_auto is not None else 0
    porcentaje_total_vehiculos = obtener_valor_seguro(fila_auto_moto, indice_zona) if fila_auto_moto is not None else 0

    # Debug: imprimir valores para verificar
    print(f"Zona: {zona_seleccionada}")
    print(f"Solo auto: {porcentaje_solo_auto}")
    print(f"Total vehículos: {porcentaje_total_vehiculos}")

    # Calcular categorías
    porcentaje_sin_vehiculo = max(0, 100 - porcentaje_total_vehiculos)
    # Asumiendo que la diferencia entre total y solo auto son motos/otros
    porcentaje_moto_otros = max(0, porcentaje_total_vehiculos - porcentaje_solo_auto)

    # Verificar que tengamos datos válidos
    if porcentaje_total_vehiculos == 0:
        # Si todos los valores son 0, mostrar mensaje de error
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, f'Datos no válidos para vehículos en {zona_seleccionada}',
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
        return fig

    # Crear gráfico de dona
    fig, ax = plt.subplots(figsize=(8, 8))

    # Preparar datos para el gráfico (solo incluir categorías con valores > 0)
    datos = []
    etiquetas = []
    colores_usados = []

    if porcentaje_solo_auto > 0:
        datos.append(porcentaje_solo_auto)
        etiquetas.append(f'Solo Auto ({porcentaje_solo_auto:.1f}%)')
        colores_usados.append('#4CAF50')  # Verde

    if porcentaje_moto_otros > 0:
        datos.append(porcentaje_moto_otros)
        etiquetas.append(f'Moto/Otros ({porcentaje_moto_otros:.1f}%)')
        colores_usados.append('#FF9800')  # Naranja

    if porcentaje_sin_vehiculo > 0:
        datos.append(porcentaje_sin_vehiculo)
        etiquetas.append(f'Sin Vehículo ({porcentaje_sin_vehiculo:.1f}%)')
        colores_usados.append('#F44336')  # Rojo

    # Si no hay datos válidos después de filtrar
    if not datos:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, f'No hay datos de vehículos para mostrar en {zona_seleccionada}',
               transform=ax.transAxes, ha='center', va='center', fontsize=14)
        ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}', fontsize=16)
        ax.axis('off')
        return fig

    # Crear gráfico de dona
    wedges, texts, autotexts = ax.pie(
        datos,
        labels=etiquetas,
        colors=colores_usados,
        autopct='%1.1f%%',
        startangle=90,
        textprops={'fontsize': 10, 'fontweight': 'bold'},
        wedgeprops={'width': 0.5, 'edgecolor': 'white', 'linewidth': 2}  # Esto crea el efecto de dona
    )

    # Mejorar el texto de los porcentajes
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(12)

    # Añadir información en el centro
    ax.text(0, 0, f'Total con\nvehículo:\n{porcentaje_total_vehiculos:.1f}%',
           ha='center', va='center', fontsize=14, fontweight='bold',
           bbox=dict(boxstyle="round,pad=0.3", facecolor='lightgray', alpha=0.8))

    ax.set_title(f'Tenencia de Vehículos - {zona_seleccionada}',
                fontsize=16, fontweight='bold', pad=20)

    plt.tight_layout()
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
