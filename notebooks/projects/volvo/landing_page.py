import streamlit as st

def display_landing_page():
    st.set_page_config(page_title="Proyecto Volvo - Caminabilidad", layout="wide")

    st.title("De la Accesibilidad a la Comprensión de la Caminabilidad 🚶‍♂️🌎")

    st.subheader("Aliados")
    st.write("**Universidad EAFIT** (Colombia) y **Tecnológico de Monterrey** (México)")

    st.subheader("Financiación")
    st.write("Volvo Research and Educational Foundations (VREF)")

    st.markdown("---")

    st.header("¿Por qué este proyecto?")
    st.write(
        "Caminar es una de las formas más comunes y sostenibles de desplazamiento urbano. "
        "Sin embargo, los entornos peatonales en ciudades latinoamericanas como Medellín y Guadalajara "
        "presentan desafíos importantes, en parte porque las herramientas tradicionales de planeación no "
        "capturan cómo las personas perciben realmente las condiciones para caminar.\n"
        "\nEste proyecto propone una nueva forma de entender y medir la caminabilidad que va más allá "
        "de los indicadores técnicos. Integramos datos objetivos sobre el entorno urbano con las percepciones "
        "de las personas que caminan por esos espacios."
    )

    st.header("¿Qué hacemos?")
    st.markdown("""
    - **Indicadores físicos:** Densidad de población, diseño de calles, disponibilidad de aceras, vegetación, entre otros.
    - **Percepción ciudadana:** Qué tan seguro, cómodo y agradable es caminar en los barrios.
    
    Así, ofrecemos una medición más completa y adaptada al contexto social y urbano de nuestras ciudades.
    """)

    st.header("¿Cómo funciona?")
    st.markdown("""
    ✔ Seleccionamos variables urbanas clave bajo el enfoque **3D**: _Densidad_, _Diversidad_ y _Diseño_.  
    ✔ Recopilamos encuestas con la ciudadanía sobre su experiencia peatonal.  
    ✔ Calculamos el **Índice de Caminabilidad** para cada segmento de calle, reflejando las diferencias dentro de cada ciudad.  
    ✔ Visualizamos los resultados mediante mapas y análisis comparativos entre barrios y ciudades.
    """)

    st.header("¿Para quién es útil este trabajo?")
    st.markdown("""
    👩‍💻 Urbanistas y planificadores  
    🏛️ Gobiernos locales  
    🎓 Investigadores y estudiantes de movilidad, equidad urbana y sistemas de información geográfica (GIS)  
    🚶‍♂️ Ciudadanía interesada en mejorar los entornos peatonales  
    """)

    st.header("Impacto esperado")
    st.markdown("""
    Promover ciudades más equitativas, seguras y caminables  
    Apoyar la toma de decisiones basadas en evidencia  
    Comprender mejor cómo las personas experimentan sus trayectos cotidianos  
    """)

# Llamada de prueba (comenta esta línea si estás importando la función en otra parte de tu app)
if __name__ == "__main__":
    display_landing_page()
