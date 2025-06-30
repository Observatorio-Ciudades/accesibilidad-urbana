import streamlit as st
import pandas as pd
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

def crear_grafico_genero(df, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de distribución por género"""
    fig, ax = plt.subplots()

    # Extraer datos de % Mujeres
    fila_mujeres = extraer_fila_por_concepto(df, "% Mujeres")

    if fila_mujeres is not None:
        if titulo_ciudad == "Guadalajara":
            # Columnas 1, 2, 3 para Guadalajara (Colinas, Providencia, Miramar)
            mujeres = convertir_a_float_seguro(fila_mujeres.iloc[1:4])
            colonias = colonias_gdl
        else:  # Medellín
            # Columnas 5, 6, 7 para Medellín (Aguacatala, Floresta, Moravia)
            mujeres = convertir_a_float_seguro(fila_mujeres.iloc[5:8])
            colonias = colonias_mde

        hombres = 100 - mujeres

        df_genero = pd.DataFrame({'Mujeres': mujeres, 'Hombres': hombres}, index=colonias)
        df_genero.plot(kind='bar', stacked=True, ax=ax, color=['#E91E63', '#2196F3'])
        ax.set_title(f'Distribución por Género - {titulo_ciudad}')
        ax.set_ylabel('Porcentaje (%)')
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
    else:
        ax.text(0.5, 0.5, 'Datos no encontrados para % Mujeres', transform=ax.transAxes, ha='center')
        ax.set_title(f'Distribución por Género - {titulo_ciudad}')

    return fig


def crear_grafico_vehiculos(df, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de tenencia de vehículos"""
    fig, ax = plt.subplots()

    # Extraer datos de vehículos
    fila_auto = extraer_fila_por_concepto(df, "% con auto")
    fila_auto_moto = extraer_fila_por_concepto(df, "% con auto/moto")

    if fila_auto is not None and fila_auto_moto is not None:
        if titulo_ciudad == "Guadalajara":
            auto = convertir_a_float_seguro(fila_auto.iloc[1:4])
            auto_moto = convertir_a_float_seguro(fila_auto_moto.iloc[1:4])
            colonias = colonias_gdl
        else:  # Medellín
            auto = convertir_a_float_seguro(fila_auto.iloc[5:8])
            auto_moto = convertir_a_float_seguro(fila_auto_moto.iloc[5:8])
            colonias = colonias_mde

        df_vehiculos = pd.DataFrame({'Solo Auto': auto, 'Auto+Moto': auto_moto}, index=colonias)
        df_vehiculos.plot(kind='bar', ax=ax, color=['#4CAF50', '#FF9800'])
        ax.set_title(f'Tenencia de Vehículos - {titulo_ciudad}')
        ax.set_ylabel('Porcentaje (%)')
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
    else:
        ax.text(0.5, 0.5, 'Datos no encontrados para vehículos', transform=ax.transAxes, ha='center')
        ax.set_title(f'Tenencia de Vehículos - {titulo_ciudad}')

    return fig


def crear_grafico_transporte(df, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de medios de transporte"""
    fig, ax = plt.subplots()

    # Extraer datos de medios de transporte
    medios = ['Caminata', 'Auto', 'Bus / SITVA', 'Motocicleta']
    datos_transporte = []
    colonias = colonias_gdl if titulo_ciudad == "Guadalajara" else colonias_mde

    for medio in medios:
        fila = extraer_fila_por_concepto(df, medio)
        if fila is not None:
            if titulo_ciudad == "Guadalajara":
                valores = convertir_a_float_seguro(fila.iloc[1:4])
            else:  # Medellín
                valores = convertir_a_float_seguro(fila.iloc[5:8])
            datos_transporte.append(valores)
        else:
            # Si no encuentra el medio, agregar ceros
            datos_transporte.append(pd.Series([0, 0, 0]))

    if len(datos_transporte) > 0:
        df_transporte = pd.DataFrame(datos_transporte, index=medios, columns=colonias)

        df_transporte.T.plot(kind='bar', stacked=True, ax=ax, colormap='Set3')
        ax.set_title(f'Medios de Transporte Utilizados - {titulo_ciudad}')
        ax.set_ylabel('Porcentaje (%)')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        plt.tight_layout()
    else:
        ax.text(0.5, 0.5, 'Datos no encontrados para transporte', transform=ax.transAxes, ha='center')
        ax.set_title(f'Medios de Transporte Utilizados - {titulo_ciudad}')

    return fig


def extraer_fila_por_concepto(df, concepto_buscar):
    """Extrae una fila específica basada en el concepto en la primera o segunda columna"""
    # Limpiar el concepto que estamos buscando
    concepto_limpio = concepto_buscar.replace('​', '').replace('\u200b', '').replace('\ufeff', '').replace('\xa0', ' ').strip()

    # Buscar en primera columna
    primera_col = df.iloc[:, 0].astype(str).str.replace('​', '', regex=False).str.strip()
    mask_primera = primera_col.str.contains(concepto_limpio, case=False, na=False, regex=False)

    if mask_primera.any():
        fila_idx = mask_primera.idxmax()
        st.markdown(f'Fila idx:{fila_idx}')
        return df.iloc[fila_idx, :]

    # Si no encuentra en primera columna, buscar en segunda columna
    if df.shape[1] > 1:
        segunda_col = df.iloc[:, 1].astype(str).str.replace('​', '', regex=False).str.strip()
        mask_segunda = segunda_col.str.contains(concepto_limpio, case=False, na=False, regex=False)

        if mask_segunda.any():
            fila_idx = mask_segunda.idxmax()
            print(fila_idx)
            return df.iloc[fila_idx, :]

    return None


def crear_grafico_razones(df, colonias_gdl, colonias_mde, titulo_ciudad):
    """Crea gráfico de razones para caminar"""
    fig, ax = plt.subplots()

    # Razones para caminar
    razones = ['Cercanía al lugar', 'Por salud', 'Única alternativa',
               'Distracción /desestrés/ Gusto /Apreciación', 'Ahorrar tiempo', 'No tengo carro']
    datos_razones = []
    colonias = colonias_gdl if titulo_ciudad == "Guadalajara" else colonias_mde

    for razon in razones:
        fila = extraer_fila_por_concepto(df, razon)
        if fila is not None:
            if titulo_ciudad == "Guadalajara":
                valores = convertir_a_float_seguro(fila.iloc[1:4])
            else:  # Medellín
                valores = convertir_a_float_seguro(fila.iloc[5:8])
            datos_razones.append(valores)

    if len(datos_razones) > 0:
        nombres_cortos = ['Cercanía', 'Salud', 'Única alternativa', 'Gusto', 'Ahorrar tiempo', 'No tengo carro']
        nombres_usados = nombres_cortos[:len(datos_razones)]

        df_razones = pd.DataFrame(datos_razones, index=nombres_usados, columns=colonias)

        df_razones.T.plot(kind='bar', stacked=True, ax=ax, colormap='viridis')
        ax.set_title(f'Razones para Caminar - {titulo_ciudad}')
        ax.set_ylabel('Porcentaje (%)')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        plt.tight_layout()
    else:
        ax.text(0.5, 0.5, 'Datos no encontrados para razones', transform=ax.transAxes, ha='center')
        ax.set_title(f'Razones para Caminar - {titulo_ciudad}')

    return fig


def crear_grafico_betas_barras(betas_df, titulo_ciudad):
    """Crea gráfico de barras para los coeficientes beta"""
    fig, ax = plt.subplots(figsize=(12, 6))
    betas_df.set_index('Variable').plot(kind='bar', ax=ax, colormap='Set1')
    ax.axhline(0, color='black', linewidth=0.8)
    ax.set_ylabel("Valor beta")
    ax.set_title(f"Coeficientes beta por variable y zona - {titulo_ciudad}")
    ax.legend(title="Zona")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return fig


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
    """Muestra todos los gráficos para una ciudad específica"""
    st.subheader(f"Hallazgos de la investigación en {titulo_ciudad}")
    st.markdown(f"""En esta sección se presentan los hallazgos más relevantes de la investigación,
    basados en las encuestas realizadas para ciudad de {titulo_ciudad}. Estos hallazgos se centran
    en la percepción de caminabilidad y la calidad del espacio público en diferentes colonias de la ciudad.""")

    # Gráficos demográficos y de comportamiento
    st.pyplot(crear_grafico_genero(df, colonias_gdl, colonias_mde, titulo_ciudad))
    st.pyplot(crear_grafico_vehiculos(df, colonias_gdl, colonias_mde, titulo_ciudad))
    st.pyplot(crear_grafico_transporte(df, colonias_gdl, colonias_mde, titulo_ciudad))
    st.pyplot(crear_grafico_razones(df, colonias_gdl, colonias_mde, titulo_ciudad))

    # Sección de betas
    st.subheader(f"Betas para {titulo_ciudad}")
    st.markdown("Los betas son coeficientes que indican la relación entre las variables y el índice de caminabilidad. Van de -2 a 2, donde valores negativos indican una relación inversa y positivos una relación directa.")

    # Gráfico de barras
    st.markdown("#### Gráfico de barras por variable")
    st.pyplot(crear_grafico_betas_barras(betas_df, titulo_ciudad))

    # Gráfico radar
    st.markdown("#### Gráfico radar por zona")
    zona_key = f"zona_{titulo_ciudad.lower().replace(' ', '_')}"
    zona_seleccionada = st.selectbox("Selecciona una zona", zonas, key=zona_key)

    fig_radar = crear_grafico_radar(betas_df, zona_seleccionada, colores_dict)
    st.plotly_chart(fig_radar, use_container_width=True)
