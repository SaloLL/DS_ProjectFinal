# Procedimiento para ejecutar sistema de Recomendacion de Restaurantes desde Streamlit

# Se importan las librerias requeridas
import pandas as pd
import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Se lee el archivo con los datos de restaurantes y reseñas integrados en un mismo archivo de datos 
df_comb_sitio_review = pd.read_parquet('df_comb_sitio_review.parquet')

# Definicion de la funcion de recomendacion
def recomendacion_cliente(df, cliente_id, estado, num_recomendaciones, categoria=None):
    # 1. Filtramos las reseñas del cliente
    reseñas_cliente = df[df['user_id'] == cliente_id]['text'].dropna().unique()
    
    # 2. Obtenemos los restaurantes reseñados por el cliente
    restaurantes_revisados = df[df['user_id'] == cliente_id]['gmap_id'].unique()
    
    # 3. Filtramos las reseñas de otros clientes para los restaurantes no revisados por el cliente y
    #    solo para el Estado indicado por el cliente
    if estado and estado.lower() != 'all':
        df = df[df['estado'].str.lower() == estado.lower()]
    
    df_otras_reseñas = df[~df['gmap_id'].isin(restaurantes_revisados)]
    reseñas_otras = df_otras_reseñas['text'].dropna()

    try:
        # 4. Calculamos la similitud entre las reseñas del cliente ingresado y las de los otros clientes
        tfidf_vectorizer = TfidfVectorizer()
        tfidf_matrix_cliente = tfidf_vectorizer.fit_transform(reseñas_cliente)
        tfidf_matrix_otras = tfidf_vectorizer.transform(reseñas_otras)
        similitud = cosine_similarity(tfidf_matrix_cliente, tfidf_matrix_otras)
        
        # 5. Obtener los índices de los restaurantes con mayor similitud
        indices_top_recomendaciones = similitud.mean(axis=0).argsort()[::-1]
        restaurantes_recomendados = set()     # Usamos un conjunto para evitar la repetición de recomendaciones
        for indice in indices_top_recomendaciones:
            restaurante = df_otras_reseñas.iloc[indice]['name']
            if categoria and categoria.lower() != 'all':   # Filtramos la categoria indicada por el cliente
                if df_otras_reseñas.iloc[indice]['category'].lower() == categoria.lower():
                    restaurantes_recomendados.add(restaurante)
            else:
                restaurantes_recomendados.add(restaurante)
            if len(restaurantes_recomendados) == num_recomendaciones:     # solo el numero de recomendaciones solicitadas
                break
        restaurantes_recomendados = list(restaurantes_recomendados)

    except ValueError as e:
        print("No se encontraron datos suficientes para la recomendacion solicitada:", e)
        return []
  
    # 6. Seleccionamos la reseña con el mayor valor de "score" (obtenido del analisis de sentimiento) para cada restaurante recomendado
    recomendaciones_con_reseñas = []
    for restaurante in restaurantes_recomendados:
        if categoria and categoria.lower() != 'all':
            df_restaurante = df_otras_reseñas[(df_otras_reseñas['name'] == restaurante) & (df_otras_reseñas['category'].str.lower() == categoria.lower())]
        else:
            df_restaurante = df_otras_reseñas[df_otras_reseñas['name'] == restaurante]
        if not df_restaurante.empty:
            reseña_seleccionada = df_restaurante.sort_values(by='score', ascending=False).iloc[0]
            # Obtenemos nombre, reseña de muestra, rating y categoria de los restaurantes recomendados 
            recomendaciones_con_reseñas.append((restaurante, reseña_seleccionada['text'], reseña_seleccionada['rating'], reseña_seleccionada['category']))
    
    return recomendaciones_con_reseñas


#
# Creamoos la interfaz de usuario con Streamlit
#

# Datos de Prueba
# cliente_id = '108178792843407619493'

# Mostrar imagen de presentación
st.image('./docs/banner1.png', use_column_width=True)

# definicion de funcion main de streamlit
def main():
    st.title('Sistema de Recomendación de Restaurantes')
    
    # Usuario ingresa su ID unico
    st.write('Ingresa tu ID de cliente único:')
    cliente_id = st.text_input('Cliente ID', '')
    
    # Usuario Selecciona el número de recomendaciones que desea de una lista desplegable
    num_recomendaciones = st.selectbox('Selecciona el número de recomendaciones que deseas:', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Obtener la lista de estados únicos del dataframe para generar lista desplegable que le aparece al usuario 
    estados_unicos = df_comb_sitio_review['estado'].unique().tolist()
    # Seleccionar el estado del restaurante
    estado = st.selectbox('Selecciona el estado donde deseas la recomendacion:', estados_unicos)

    # Obtener las 30 categorías más comunes del dataframe para generar lista desplegable que le aparece al usuario 
    categorias_comunes = df_comb_sitio_review['category'].value_counts().head(30).index.tolist()
    categorias_comunes.insert(0, 'All')
    # Seleccionar la categoría del restaurante
    categoria = st.selectbox('Selecciona la categoría de restaurante:', categorias_comunes)

    if st.button('Recomendar'):
        # Llama al procedimiento de ML-Sistema de Recomendacion
        recomendaciones_con_reseñas = recomendacion_cliente(df_comb_sitio_review, cliente_id, estado, num_recomendaciones=num_recomendaciones, categoria=categoria)
        
        # Muestra los resultados en pantalla 
        st.write('Restaurantes recomendados:')
        for i, (restaurante, reseña, rating, categoria_restaurante) in enumerate(recomendaciones_con_reseñas, start=1):
            st.write(f'{i}. {restaurante}:')
            st.write(f'Categoria: {categoria_restaurante}')
            st.write(f'Muestra de Reseña: {reseña} Rating: {rating}')
            st.write('')  # Agregar una línea en blanco entre cada restaurante

if __name__ == '__main__':
    main()