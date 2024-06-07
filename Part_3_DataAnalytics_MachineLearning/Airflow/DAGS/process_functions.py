# Función que guarda el dataframe en un archivo temporal de parquet y lo sube
def dataframe_to_parquet_and_upload(source_file_name, destination_blob_name):
    import os
    import pyarrow.parquet as pq
    # import pyarrow as pa
    from google.cloud import storage
    def upload_to_gcs(source_file_name, destination_blob_name, bucket_name='archivos-preprocesados-henry'):
        """Sube un archivo a un bucket de Google Cloud Storage."""
        # Inicializa el cliente de GCS
        storage_client = storage.Client()
        # Obtén el bucket
        bucket = storage_client.bucket(bucket_name)
        # Crea un blob en el bucket
        blob = bucket.blob(destination_blob_name)
        # Sube el archivo al blob
        blob.upload_from_filename(source_file_name)
        print(f"Archivo {source_file_name} subido a {destination_blob_name}.")
        # # Guarda el DataFrame como archivo Parquet
        # table = pa.Table.from_pandas(df)
        # pq.write_table(table, 'temp.parquet')
        # Sube el archivo Parquet a Google Cloud Storage
        # upload_to_gcs(bucket_name, 'temp.parquet', destination_blob_name)
    upload_to_gcs(source_file_name, destination_blob_name)
    # Elimina el archivo temporal
    # os.remove('temp.parquet')
    return (True)

def get_json_data(folder_name):
    new_files = False
    # folder_name = 'metadata-sitios' # Here we use the folder name that we want to explore
    confirmation_name = f'readed_jsons+{folder_name}.txt'
    airflow_json_folder = f'/opt/airflow/datasets/{folder_name}'
    airflow_confirmation_folder = f'/opt/airflow/datasets/readed_names/{confirmation_name}'

    # Read confirmation file
    try:
        with open(airflow_confirmation_folder, 'r') as f:
            datanames = set(f.read().splitlines())
    except FileNotFoundError:
        datanames = set()
        print('There are no json files executed yet.\nReading all files instead.')
    except Exception as e:
        print(f'Error reading confirmation file: {str(e)}')
        return

    # Process JSON files
    try:
        import os
        import pandas as pd
    except ImportError as e:
        print(f'Packages error: {str(e)}')
        return
    dataframes = []
    try:
        for filename in os.listdir(airflow_json_folder):
            if filename.endswith('.json') and filename not in datanames:
                new_files = True
                df_temporal = pd.read_json(os.path.join(airflow_json_folder, filename), lines=True)
                dataframes.append(df_temporal)
                datanames.add(filename)
    except Exception as e:
        print(f'Error processing JSON files: {str(e)}')
        return

    # Rewrite the confirmation file if new files present
    if new_files:
        try:
            dfg_sites = pd.concat(dataframes, ignore_index=True)
        except Exception as e:
            print(f"Pandas Concat error: {str(e)}")
            return
        try:
            with open(airflow_confirmation_folder, 'w') as f:
                for line in datanames:
                    f.write(line + '\n')
        except Exception as e:
            print(f'Error writing confirmation file: {str(e)}')
            return
        else:
            try:
                dfg_sites.to_parquet('/opt/airflow/datasets/OUT/dfg_sites.parquet')
                # dfg_sites.to_csv('/opt/airflow/datasets/OUT/dataset_g_restaurants.csv', index=False)
                return(True)
            except Exception as e:
                print(f"Error exporting dataframes: {str(e)}")
                return
    else:
        print('No new json files detected')
        return(False)
    

def get_rest_data():
    try:
        import pandas as pd
        dfg_sites = pd.read_parquet('/opt/airflow/datasets/OUT/dfg_sites.parquet')
        # Se obtiene la categoría de los negocios.
        negocios_cat = dfg_sites[["gmap_id","category"]].dropna()
        # Se separan las categorías de los negocios
        negocios_cat = negocios_cat.explode("category")
        id_rest = negocios_cat[negocios_cat["category"].str.contains('restaurant|bar', case=False)]
        no = ['Barber shop', 'Bark supplier',
        'Barber supply store', 'Eyebrow bar',
        'Hookah bar', 'Bartending school',
        'Barber school', 'Subaru dealer', 'Barrister', 'Bar stool supplier',
        'Cabaret club', 'Barbecue area', 'Hyperbaric medicine physician',
        'Bar restaurant furniture store', 'Barrel supplier',
        'Bariatric surgeon', 'Military barracks', 'Army barracks'
        ]
        id_rest_final = id_rest[~id_rest["category"].isin(no)]
        dfg_rest = dfg_sites[dfg_sites["gmap_id"].isin(id_rest_final["gmap_id"])]
        dfg_rest.to_parquet('/opt/airflow/datasets/OUT/dfg_rest.parquet')
        return(True)
    except Exception as e:
        print(e)
        return(False)
    
def etl_g_2():
    try:
        import pandas as pd
        dfg_rest = pd.read_parquet('/opt/airflow/datasets/OUT/dfg_rest.parquet')
        # Adecuación de tipos de dato
        dfg_rest = dfg_rest[['name', 'address', 'gmap_id', 'latitude', 'longitude', 'avg_rating', 'num_of_reviews', 'price']]
        dfg_rest = dfg_rest.rename(columns={'address': 'address_full'})
        dfg_rest[['address', 'city', 'postal_code']] = dfg_rest['address_full'].str.extract(r'.*,\s*([^,]+),\s*([^,]+),\s*([^,]+)')
        dfg_rest['state'] = dfg_rest['postal_code'].str.split(' ').str[0]
        dfg_rest['postal_code'] = dfg_rest['postal_code'].str.split(' ').str[1]
        dfg_rest['postal_code'] = pd.to_numeric(dfg_rest['postal_code'], errors='coerce')
        dfg_rest = dfg_rest.dropna(subset=['postal_code'])
        dfg_rest['postal_code'] = dfg_rest['postal_code'].astype(int)
        # Convertir precio a formato numerico
        price_mapping = {'$': 1, '$$': 2, '$$$': 3, '$$$$': 4}
        dfg_rest['price'] = dfg_rest['price'].map(price_mapping)
        # Reemplazar valores nulos del campo "precio" con el promedio de precios basado en cercanía, utilzando el código postal
        dfg_rest['postal_code'] = dfg_rest['postal_code'].astype(str)
        average_price_by_zip = dfg_rest.groupby('postal_code')['price'].apply(lambda x: x.dropna().astype(float).mean())
        dfg_rest['price'] = dfg_rest.apply(lambda row: average_price_by_zip.get(row['postal_code']) if pd.isnull(row['price']) else row['price'], axis=1)
        dfg_rest = dfg_rest.dropna(subset=['price'])
        # Subida del dataset a GCS
        destino = 'dfg_rest.parquet'
        dfg_rest.to_parquet('/opt/airflow/datasets/OUT/dfg_rest.parquet')
        dataframe_to_parquet_and_upload(dfg_rest,destino)
        return(True)
    except Exception as e:
        print(e)
        return(False)

def category_creation_ds():
    try:
        import pandas as pd
        dfg_restaurants = pd.read_parquet('/opt/airflow/datasets/OUT/dfg_rest.parquet')
        dfg_categories = dfg_restaurants[['gmap_id', 'category']]
        dfg_categories = dfg_categories.explode('category')
        dfg_categories.rename(columns={'gmap_id': 'site_id'}, inplace=True)
        dfg_categories_grouped = dfg_categories.groupby(['site_id'])['category'].count().reset_index()
        dfg_categories_grouped.sort_values(by='category', ascending=False, inplace=True)
        dfg_categories.to_parquet('/opt/airflow/datasets/OUT/dfg_site_categories.parquet')
        return(True)
    except Exception as e:
        print(e)
        return(False)
    
def attributes_ds_creation():
    try:
        import pandas as pd
        dfg_restaurants = pd.read_parquet("/opt/airflow/datasets/OUT/dfg_rest.parquet")
        dfg_misc = dfg_restaurants[['gmap_id', 'MISC']]
        dfg_misc.rename(columns={'gmap_id': 'site_id'}, inplace=True)
        # Se obtiene un dataframe con los datos normalizados de "MISC"
        x = pd.json_normalize(dfg_misc["MISC"])
        #Se concatenan los dos dataframes.
        dfg_misc = pd.concat([dfg_misc.reset_index(drop=True), x.reset_index(drop=True)], axis=1)
        # Se elimina la columna "MISC"
        dfg_misc.drop(columns=["MISC"],inplace=True)
        # Se procede a pasar los datos de las columnas accesibility, activities y amenities a diferentes dataframes 
        dfg_attributes_accessibility = dfg_misc[["site_id","Accessibility"]]
        dfg_attributes_activities = dfg_misc[["site_id","Activities"]]
        dfg_attributes_amenities = dfg_misc[["site_id","Amenities"]]
        # Se eliminan datos nulos
        dfg_attributes_accessibility.dropna(inplace=True)
        dfg_attributes_activities.dropna(inplace=True)
        dfg_attributes_amenities.dropna(inplace=True)
        # Se explotan los datos dentro de las columnas de las categorías
        dfg_attributes_accessibility = dfg_attributes_accessibility.explode("Accessibility")
        dfg_attributes_activities = dfg_attributes_activities.explode("Activities")
        dfg_attributes_amenities = dfg_attributes_amenities.explode("Amenities")
        # Se resetean índices para asegurarse de que sean únicos
        dfg_attributes_accessibility = dfg_attributes_accessibility.reset_index(drop=True)
        dfg_attributes_activities = dfg_attributes_activities.reset_index(drop=True)
        dfg_attributes_amenities = dfg_attributes_amenities.reset_index(drop=True)
        # Se resetean índices para asegurarse de que sean únicos
        dfg_attributes_accessibility = dfg_attributes_accessibility.rename(columns={"Accessibility":"attributes"})
        dfg_attributes_activities = dfg_attributes_activities.rename(columns={"Activities":"attributes"})
        dfg_attributes_amenities = dfg_attributes_amenities.rename(columns={"Amenities":"attributes"})
        # Se unen los datos en el mismo dataframe
        dfg_attributes = pd.concat([dfg_attributes_accessibility, 
                                dfg_attributes_activities, 
                                dfg_attributes_amenities], axis=0, ignore_index=True)
        dfg_attributes.to_parquet("/opt/airflow/datasets/OUT/dfg_attributes.parquet")
        return(True)
    except Exception as e:
        print(e)
        return(False)
    

def prices_ds_creation():
    try:
        import pandas as pd
        dfg_rest = pd.read_parquet("/opt/airflow/datasets/OUT/dfg_rest.parquet")
        dfg_rest_prices_by_zip = dfg_rest.groupby('postal_code')['price'].apply(lambda x: x.dropna().astype(float).mean())
        dfg_rest_prices_by_zip_df = dfg_rest_prices_by_zip.to_frame()
        dfg_rest_prices_by_zip_df.to_parquet("/opt/airflow/datasets/OUT/dfg_rest_prices_by_zip.parquet")
        return(True)
    except Exception as e:
        print(e)
        return(False)
    
def coord_ds_create():
    try:
        import pandas as pd
        import math
        dfg_rest = pd.read_parquet("/opt/airflow/datasets/OUT/dfg_rest.parquet")
        dfg_rest_coord = dfg_rest[['gmap_id', 'latitude', 'longitude', 'name', 'state', 'city', 'postal_code',]]
        dfg_rest_coord = dfg_rest_coord.rename(columns={'gmap_id': 'business_id'})
        dfg_rest_coord['source'] = 'google'
        # Función para convertir latitud y longitud a coordenadas cartesianas
        def lat_lon_to_cartesian(lat, lon):
            R = 6371  # Radio de la Tierra en kilómetros
            x = R * math.cos(math.radians(lat)) * math.cos(math.radians(lon))
            y = R * math.cos(math.radians(lat)) * math.sin(math.radians(lon))
            return x, y
        # Suponiendo que tienes un DataFrame llamado df con las columnas business_id, latitud y longitud
        # Agrega campos de coordenadas cartesianas x e y al DataFrame
        dfg_rest_coord['x'], dfg_rest_coord['y'] = zip(*dfg_rest_coord.apply(lambda row: lat_lon_to_cartesian(row['latitude'], row['longitude']), axis=1))
        dfg_rest_coord.to_parquet('/opt/airflow/datasets/OUT/dfg_rest_coord.parquet')
        return(True)
    except Exception as e:
        print(e)
        return(False)
    
def reviews_datasets():
    try:
        import pandas as pd
        import os
        dataframes_filtrados_rest = []
        dfg_rest = pd.read_parquet("/opt/airflow/datasets/OUT/dfg_rest.parquet")
        dfg_rest_ids = dfg_rest[['gmap_id', 'state']]
        # Función para procesar cada archivo JSON
        def procesar_archivo_json(filepath):
            try:
                df = pd.read_json(filepath, lines=True)
                # Filtrar las filas según los gmap_ids deseados
                df_filtrado = df[df['gmap_id'].isin(dfg_rest_ids["gmap_id"])]
                dataframes_filtrados_rest.append(df_filtrado)
            except Exception as e:
                print(f"Error al procesar el archivo {filepath}: {e}")
        estados_ruta = {
            "AL":"/opt/airflow/datasets/reviews-estados/review-Alabama/review-Alabama",
            'AK':'/opt/airflow/datasets/reviews-estados/review-Alaska/review-Alaska',
            'AZ':'/opt/airflow/datasets/reviews-estados/review-Arizona/review-Arizona',
            'AR':'/opt/airflow/datasets/reviews-estados/review-Arkansas/review-Arkansas',
            'CA':'/opt/airflow/datasets/reviews-estados/review-California/review-California',
            'CO':'/opt/airflow/datasets/reviews-estados/review-Colorado/review-Colorado',
            'CT':'/opt/airflow/datasets/reviews-estados/review-Connecticut/review-Connecticut',
            'DE':"/opt/airflow/datasets/reviews-estados/review-Delaware/review-Delaware",
            'District_of_Columbia':"/opt/airflow/datasets/reviews-estados/review-District_of_Columbia",
            'FL':"/opt/airflow/datasets/reviews-estados/review-Florida/review-Florida",
            'GA':"/opt/airflow/datasets/reviews-estados/review-Georgia/review-Georgia",
            'HI':"/opt/airflow/datasets/reviews-estados/review-Hawaii/review-Hawaii",
            'ID':"/opt/airflow/datasets/reviews-estados/review-Idaho/review-Idaho",
            'IL':"/opt/airflow/datasets/reviews-estados/review-Illinois/review-Illinois",
            'IN':"/opt/airflow/datasets/reviews-estados/review-Indiana/review-Indiana",
            'IA':"/opt/airflow/datasets/reviews-estados/review-Iowa/review-Iowa",
            'KS':"/opt/airflow/datasets/reviews-estados/review-Kansas/review-Kansas",
            'KY':"/opt/airflow/datasets/reviews-estados/review-Kentucky/review-Kentucky",
            'LA':"/opt/airflow/datasets/reviews-estados/review-Louisiana/review-Louisiana",
            'ME':"/opt/airflow/datasets/reviews-estados/review-Maine/review-Maine",
            'MD':"/opt/airflow/datasets/reviews-estados/review-Maryland/review-Maryland",
            'MA':"/opt/airflow/datasets/reviews-estados/review-Massachusetts/review-Massachusetts",
            'MI':"/opt/airflow/datasets/reviews-estados/review-Michigan/review-Michigan",
            'MN':"/opt/airflow/datasets/reviews-estados/review-Minnesota/review-Minnesota",
            'MS':"/opt/airflow/datasets/reviews-estados/review-Mississippi/review-Mississippi",
            'MO':"/opt/airflow/datasets/reviews-estados/review-Missouri/review-Missouri",
            'MT':"/opt/airflow/datasets/reviews-estados/review-Montana/review-Montana",
            'NE':"/opt/airflow/datasets/reviews-estados/review-Nebraska/review-Nebraska",
            'NV':"/opt/airflow/datasets/reviews-estados/review-Nevada/review-Nevada",
            'NH':"/opt/airflow/datasets/reviews-estados/review-New_Hampshire/review-New_Hampshire",
            'NJ':"/opt/airflow/datasets/reviews-estados/review-New_Jersey/review-New_Jersey",
            'NM':"/opt/airflow/datasets/reviews-estados/review-New_Mexico/review-New_Mexico",
            'NY':"/opt/airflow/datasets/reviews-estados/review-New_York/review-New_York",
            'NC':"/opt/airflow/datasets/reviews-estados/review-North_Carolina/review-North_Carolina",
            'ND':"/opt/airflow/datasets/reviews-estados/review-North_Dakota/review-North_Dakota",
            'OH':"/opt/airflow/datasets/reviews-estados/review-Ohio/review-Ohio",
            'OK':"/opt/airflow/datasets/reviews-estados/review-Oklahoma/review-Oklahoma",
            'OR':"/opt/airflow/datasets/reviews-estados/review-Oregon/review-Oregon",
            'PA':"/opt/airflow/datasets/reviews-estados/review-Pennsylvania/review-Pennsylvania",
            'RI':"/opt/airflow/datasets/reviews-estados/review-Rhode_Island/review-Rhode_Island",
            'SC':"/opt/airflow/datasets/reviews-estados/review-South_Carolina/review-South_Carolina",
            'SD':"/opt/airflow/datasets/reviews-estados/review-South_Dakota/review-South_Dakota",
            'TN':"/opt/airflow/datasets/reviews-estados/review-Tennessee/review-Tennessee",
            'TX':"/opt/airflow/datasets/reviews-estados/review-Texas/review-Texas",
            'UT':"/opt/airflow/datasets/reviews-estados/review-Utah/review-Utah",
            'VT':"/opt/airflow/datasets/reviews-estados/review-Vermont/review-Vermont",
            'VA':"/opt/airflow/datasets/reviews-estados/review-Virginia/review-Virginia",
            'WA':"/opt/airflow/datasets/reviews-estados/review-Washington/review-Washington",
            'WV':"/opt/airflow/datasets/reviews-estados/review-West_Virginia/review-West_Virginia",
            'WI':"/opt/airflow/datasets/reviews-estados/review-Wisconsin/review-Wisconsin",
            'WY':"/opt/airflow/datasets/reviews-estados/review-Wyoming/review-Wyoming"
        }
        # Iterar sobre el diccionario y procesar los archivos
        for state, ruta_directorio in estados_ruta.items():
            # Procesar cada archivo en el directorio
            for filename in os.listdir(ruta_directorio):
                if filename.endswith('.json'):
                    filepath = os.path.join(ruta_directorio, filename)
                    procesar_archivo_json(filepath)
        # Concatenar todos los dataframes en uno solo
        dfg_reviews = pd.concat(dataframes_filtrados_rest, ignore_index=True)
        # Se eliminan los campos de pics y resp.
        dfg_reviews.drop(columns=["pics","resp"],inplace=True)
        # preprocesado reviews 
        dfg_reviews.to_parquet("/opt/airflow/datasets/OUT/reviews-estados-parquet/dfg_reviews_preprocesados.parquet")
        return(True)
    except Exception as e:
        print(e)
        return(False)
    
def preprocess_reviews():
    try:
        import pandas as pd
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        dfg_reviews = pd.read_parquet('/opt/airflow/datasets/OUT/reviews-estados-parquet/dfg_reviews_preprocesados.parquet')
        dfg_reviews_usa = dfg_reviews
        dfg_reviews_usa['time'] = dfg_reviews_usa['time'] / 1000
        # Convertir la marca de tiempo a un objeto de fecha y hora
        dfg_reviews_usa['datetime'] = dfg_reviews_usa['time'].apply(lambda x: datetime.fromtimestamp(x))
        dfg_reviews_sent = dfg_reviews_usa
        # Análisis de sentimientos con VADER
        analyzer = SentimentIntensityAnalyzer()
        # Aplicar análisis de sentimientos, manejando valores nulos
        dfg_reviews_sent['vader_polarity'] = dfg_reviews_sent['text'].apply(
            lambda text: analyzer.polarity_scores(text)['compound'] if pd.notnull(text) else 0
        )
        # Clasificar sentimientos según la polaridad
        dfg_reviews_sent['vader_sentiment'] = pd.cut(
            dfg_reviews_sent['vader_polarity'], 
            bins=[-float('inf'), -0.001, 0.0, float('inf')], 
            labels=[-1, 0, 1]
        )
        # Se obtiene el dataset final con las columnas requeridas.
        dfg_reviews = dfg_reviews_usa[['user_id', 'gmap_id', 'datetime', 'rating', 'vader_polarity', 'vader_sentiment']]
        dfg_reviews_usa.to_parquet('/opt/airflow/datasets/OUT/reviews-estados-parquet/dfgrevall.parquet')
        return(True)
    except Exception as e:
        print(e)
        return(False)