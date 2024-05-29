from google.cloud import bigquery

import pandas as pd
import numpy as np

# Autentica tu cuenta de GCP
client = bigquery.Client()

# Se cargan los datos de Big Query
dataset_id = "proyecto-nuevo-423502.Data_Henry_Final"

# Restaurantes
table_id_restaurantes = "proyecto-nuevo-423502.Data_Henry_Final.restaurantes"

# Usuarios
table_id_usuarios = "proyecto-nuevo-423502.Data_Henry_Final.usuarios"

# Checkins
# table_id_checkins = "proyecto-nuevo-423502.Data_Henry_Final.checkins"

# Reviews
table_id_reviews = "proyecto-nuevo-423502.Data_Henry_Final.reviews"

# Categorías
table_id_categorias = "proyecto-nuevo-423502.Data_Henry_Final.categorias"

# Se cargan los datasets

# Listado unívoco de locales de ambos datasets
data_dfgy_rest_uniques = "gs://archivos-pre-procesados/dfgy_rest_uniques.parquet"
dfgy_rest_uniques = pd.read_parquet(data_dfgy_rest_uniques)

# Yelp

## restaurante
data_yelp_rest = "gs://archivos-pre-procesados/dfy_rest.parquet"
yelp_rest = pd.read_parquet(data_yelp_rest)

## user
data_yelp_user = "gs://archivos-pre-procesados/dfy_user.parquet"
yelp_user = pd.read_parquet(data_yelp_user)

## checkins
# data_yelp_checkins = "gs://archivos-pre-procesados/dfy_checkins.parquet"
# yelp_checkins = pd.read_parquet(data_yelp_checkins)


## reviews
data_yelp_reviews = "gs://archivos-pre-procesados/dfy_reviews.parquet"
yelp_reviews = pd.read_parquet(data_yelp_reviews)


## tips
data_yelp_tips = "gs://archivos-pre-procesados/dfy_tips.parquet"
yelp_tips = pd.read_parquet(data_yelp_tips)


## categorías
data_yelp_site_categories = "gs://archivos-pre-procesados/dfy_site_categories.parquet"
yelp_site_categories = pd.read_parquet(data_yelp_site_categories)


## atributos
data_yelp_site_attributes = "gs://archivos-pre-procesados/dfy_attributes.parquet"
yelp_site_attributes = pd.read_parquet(data_yelp_site_attributes)

# Google

## Restaurantes
data_google_rest = "gs://archivos-pre-procesados/dfg_rest.parquet"
google_rest = pd.read_parquet(data_google_rest)

## Reviews
data_google_reviews = "gs://archivos-pre-procesados/dfg_reviews.parquet"
google_reviews = pd.read_parquet(data_google_reviews)

## Categorías
data_google_site_categories = "gs://archivos-pre-procesados/dfg_site_categories.parquet"
google_site_categories = pd.read_parquet(data_google_site_categories)

# Procesamiento de la data

## DataSet de Yelp
### Reviews
dfy_reviews = yelp_reviews.copy()

dfy_reviews['source'] = 'yelp'
dfy_reviews = dfy_reviews[['source','business_id','user_id', 'date', 'month', 'year', 'stars', 'polarity', 'sentiment']]
dfy_reviews = dfy_reviews.rename(columns={'business_id': 'site_id', 'date':'datetime', 'stars':'rating'})

### Restaurantes
dfy_rest = yelp_rest

dfy_rest['source'] = 'yelp'
dfy_rest = dfy_rest[['source','business_id', 'name', 'state', 'city', 'postal_code', 'price', 'stars', 'review_count']]
dfy_rest = dfy_rest.rename(columns={'business_id': 'site_id', 'stars':'rating_avg', 'review_count':'reviews_count'})

dfy_rest.dropna(subset=['price'], inplace=True)

    # Agregado de fecha de inicio de acitividad
dfy_rest_date_start = dfy_reviews.groupby('site_id')['datetime'].min().reset_index()
dfy_rest = pd.merge(dfy_rest, dfy_rest_date_start, how='left', on=['site_id'])
dfy_rest = dfy_rest.rename(columns={'datetime': 'date_start'})

dfy_rest['year'] = dfy_rest['date_start'].dt.year
dfy_rest['month'] = dfy_rest['date_start'].dt.month

dfy_rest['state_city'] = dfy_rest['state'].str.cat(dfy_rest['city'], sep=' - ')
dfy_rest['city_postalcode'] = dfy_rest['city'].str.cat(dfy_rest['postal_code'], sep=' - ')
dfy_rest['state_city_postalcode'] = dfy_rest['state'].str.cat(dfy_rest['city'], sep=' - ').str.cat(dfy_rest['postal_code'], sep=' - ')

### Usuarios
dfy_user =yelp_user.copy()

dfy_user = dfy_user[['user_id', 'review_count', 'yelping_since', 'average_stars']]
dfy_user = dfy_user.rename(columns={'review_count':'reviews_count', 'yelping_since':'date_start', 'average_stars':'rating_avg'})

dfy_user['date_start'] = pd.to_datetime(dfy_user['date_start'], errors='coerce')
dfy_user.reset_index(drop=True, inplace=True)

dfy_user['year'] = dfy_user['date_start'].dt.year
dfy_user['month'] = dfy_user['date_start'].dt.month

### Checkins 
# dfy_checkins = yelp_checkins[['business_id', 'date']]
# dfy_checkins = dfy_checkins.rename(columns={'business_id':'site_id', 'date':'datetime'})

    #dfy_checkins['datetime'] = pd.to_datetime(dfy_checkins['date'])
# dfy_checkins['year'] = dfy_checkins['datetime'].dt.year
# dfy_checkins['month'] = dfy_checkins['datetime'].dt.month

# dfy_checkins['source'] = 'yelp'

# dfy_checkins = dfy_checkins[['source', 'site_id', 'datetime', 'year', 'month']]

### Categorías
dfy_categories = yelp_site_categories[['site_id', 'categories']]
dfy_categories['source'] = 'yelp'
dfy_categories = dfy_categories[['source', 'site_id', 'categories']]

## Dataset de Google

### Reviews
dfg_reviews = google_reviews
dfg_reviews['source'] = 'google'
dfg_reviews['month'] = dfg_reviews['datetime'].dt.month
dfg_reviews['year'] = dfg_reviews['datetime'].dt.year
dfg_reviews = dfg_reviews[['source', 'gmap_id','user_id', 'datetime', 'month', 'year', 'rating', 'vader_polarity', 'vader_sentiment']]
dfg_reviews = dfg_reviews.rename(columns={'gmap_id': 'site_id', 'vader_polarity':'polarity', 'vader_sentiment':'sentiment'})

### Restaurantes
dfg_rest = google_rest
dfg_rest['source'] = 'google'
dfg_rest = dfg_rest[['source','gmap_id', 'name', 'state', 'city', 'postal_code', 'price', 'avg_rating', 'num_of_reviews']]
dfg_rest = dfg_rest.rename(columns={'gmap_id': 'site_id', 'avg_rating':'rating_avg', 'num_of_reviews':'reviews_count'})

    # Agregado de fecha de inicio de acitividad
dfg_rest_date_start = dfg_reviews.groupby('site_id')['datetime'].min().reset_index()
dfg_rest = pd.merge(dfg_rest, dfg_rest_date_start, how='left', on=['site_id'])
dfg_rest = dfg_rest.rename(columns={'datetime': 'date_start'})

dfg_rest['year'] = dfg_rest['date_start'].dt.year
dfg_rest['month'] = dfg_rest['date_start'].dt.month

dfg_rest['state_city'] = dfg_rest['state'].str.cat(dfg_rest['city'], sep=' - ')
dfg_rest['city_postalcode'] = dfg_rest['city'].str.cat(dfg_rest['postal_code'], sep=' - ')
dfg_rest['state_city_postalcode'] = dfg_rest['state'].str.cat(dfy_rest['city'], sep=' - ').str.cat(dfg_rest['postal_code'], sep=' - ')

### User (se crea el dataset)
dfg_user = dfg_reviews.groupby(['user_id']).agg({'site_id': 'count', 'datetime': 'min', 'rating': 'mean'}).reset_index()
dfg_user = dfg_user.rename(columns={'site_id':'reviews_count', 'datetime':'date_start', 'rating':'rating_avg'})
dfg_user['year'] = dfg_user['date_start'].dt.year
dfg_user['month'] = dfg_user['date_start'].dt.month

### Checkins (se crea el dataset)
# dfy_checkins = yelp_checkins[['business_id', 'date']]
# dfy_checkins = dfy_checkins.rename(columns={'business_id':'site_id', 'date':'datetime'})

    #dfy_checkins['datetime'] = pd.to_datetime(dfy_checkins['date'])
# dfy_checkins['year'] = dfy_checkins['datetime'].dt.year
# dfy_checkins['month'] = dfy_checkins['datetime'].dt.month

# dfy_checkins['source'] = 'yelp'

# dfy_checkins = dfy_checkins[['source', 'site_id', 'datetime', 'year', 'month']]
# dfg_checkins = dfg_reviews[['source', 'site_id', 'datetime', 'year', 'month']]

### Categorías
dfg_categories = google_site_categories.copy()

dfg_categories.rename(columns={'category': 'categories'}, inplace=True)
dfg_categories['source'] = 'google'
dfg_categories = dfg_categories[['source', 'site_id', 'categories']]

# Combinación de los datasets

## Restaurantes
    # Filtrado
dfy_rest = dfy_rest[dfy_rest['site_id'].isin(dfgy_rest_uniques['business_id'])]
    # Union
dfgy_rest = pd.concat([dfy_rest, dfg_rest])
    # Eliminación de duplicados
dfgy_rest= dfgy_rest.drop_duplicates(subset=['site_id'], keep='first')
    # Procesamiento del tipo de dato.
dfgy_rest[["source",'site_id',"name","state","city","state_city","city_postalcode","state_city_postalcode"]] = dfgy_rest[["source",'site_id',"name","state","city","state_city","city_postalcode","state_city_postalcode"]].astype(str)
dfgy_rest[["postal_code","year","month"]] = dfgy_rest[["postal_code","year","month"]].astype(float).fillna(0).astype(int)


## User
dfgy_user = pd.concat([dfy_user, dfg_user])
    # Procesamiento del tipo de dato.
dfgy_user['user_id'] = dfgy_user['user_id'].astype(str)
    #Eliminación de duplicados
dfgy_user = dfgy_user.drop_duplicates(subset=['user_id'], keep='first')

## Categorías
    # Filtrado
dfy_categories = dfy_categories[dfy_categories['site_id'].isin(dfgy_rest_uniques['business_id'])]

    # Union
dfgy_categories = pd.concat([dfy_categories, dfg_categories])
    
    # Procesamiento del tipo de dato.
dfgy_categories[['categories',"site_id","source"]] = dfgy_categories[['categories',"site_id","source"]].astype(str)

## Checkins
    # Filtrado
# dfy_checkins = dfy_checkins[dfy_checkins['site_id'].isin(dfgy_rest_uniques['business_id'])]

    # Union
# dfgy_checkins = pd.concat([dfy_checkins, dfg_checkins])

    # Procesamiento del tipo de dato.
# dfgy_checkins[["source",'site_id']] = dfgy_checkins[["source",'site_id']].astype(str)

## Reviews
    # Filtrado
dfy_reviews = dfy_reviews[dfy_reviews['site_id'].isin(dfgy_rest_uniques['business_id'])]

    # Union
dfgy_reviews = pd.concat([dfy_reviews, dfg_reviews])
    # Procesamiento del tipo de dato.
dfgy_reviews[['user_id',"site_id","source"]] = dfgy_reviews[['user_id',"site_id","source"]].astype(str)

# Se crean dos diccionarios para relacionar a cada uno de los dataframes procesados con su tabla correspondiente en BigQuery

# Relacionamiento del DataFrames al ID de la tabla correspondiente en BigQuery
dataframe_to_table_map = {
    "dataframe_restaurantes": table_id_restaurantes,
    "dataframe_usuarios": table_id_usuarios,
    # "dataframe_checkins": table_id_checkins,
    "dataframe_reviews": table_id_reviews,
    "dataframe_categorias": table_id_categorias,
}

# Obtención de los DataFrames finales
dataframes = {
    "dataframe_restaurantes": dfgy_rest,
    "dataframe_usuarios": dfgy_user,
    "dataframe_reviews": dfgy_reviews,
    "dataframe_categorias": dfgy_categories,
    # "dataframe_checkins": dfgy_checkins
}

# Carga de cada uno de los DataFrame en su tabla correspondiente.

for dataframe_name, table_id in dataframe_to_table_map.items():
    # Obtener el DataFrame desde la variable.
    dataframe = dataframes[dataframe_name]

    # Se configura el trabajo de carga que se utilizará para subir la información a las tablas de BiqQuery.
    job_config = bigquery.LoadJobConfig() # Se define el objeto que permitirá la subida de la información.
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Permite sobrescribir la tabla si ya existe.

    # Cargar el DataFrame en la tabla de BigQuery.
    load_job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    load_job.result()  # Esperar a que el trabajo de carga se complete

    print(f"Successfully loaded {dataframe_name} into table {table_id}")