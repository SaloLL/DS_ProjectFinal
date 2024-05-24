# DS_ProjectFinal

##  Contexto general del trabajo

El trabajo constituye una propuesta real de solución a un problema real de la industria. En particular, se define un sistema de recomendación como producto final que utiliza técnicas de machine learning. La industria a aplicar este sistema es el conjunto de restaurantes y bares de Estados Unidos. 

Se utilizará como información de entrada a los usuarios, locales y reseñas registradas en los datasets brindadados de YELP y GOOGLE. Se apunta a un tipo de cliente que tiene un perfil de propietario y/o directivo de restaurantes y bares, cuyo interés es obtener información clave sobre las tendencias e intereses de los clientes del sector, con el propósito de que ésta sirva como input para sus proyectos y estrategias que mejoren el desempeño de su marca.

## Contenidos y estructura
* Sprint 01. Puesta en Marcha: Definición del proyecto y análisis exploratorio de datos. Entregables: 
    * Charter del proyecto (incluye fundamentos del proyecto, stack tecnológico y flujo de trabajo, Alcance, objetivo, entregables)
    * EDA (Exploratory Data Analysis)
* Sprint 02. Data Engineeering: Implementación de estructura de datos. Entregables: 
    * Reporte de Data Engineering y producto Machine Learning
* Sprint 03. Data Analytics & Machine Learning: Desarrollo final de la herramienta de análisis de la información y el producto de Machine Learning. Entregables:
    * Dashboard final
    * Producto ML

## Resumen de Sprint 01
Durante el primer sprint, se definen las bases del proyecto y se realiza el análisis exploratorio de datos. Este análisis nos permitirá conocer las bases del mercado en cuestión, conocer las principales características de oferta y demanda, así como las preferencias del usuario. Asimismo, nos traerá una idea para definir los siguientes puntos:

* Objetivo del proyecto
* Alcance del proyecto
* Solución Propuesta
* Estructura, duración y entregables
* Stack tecnológico
* Datasets a utilizar

Para la realización del EDA, se realiza en forma previa la fase de ETL (Extraction, transformation and loading of data). Debido a la gran cantidad de información y necesidad de procesamiento, el ETL se realiza en forma separada con los siguientes archivos:
* DS_PjFinal_yelp1_sites_checkins_users
* DS_PjFinal_yelp2_reviews
* DS_PjFinal_google1_sites
* DS_PjFinal_google2_reviews
* DS_PjFinal_ETL_SitesUniques
	
La estructura general y contenido propuesto para cada parte del EDA consiste en:
* Importación de librerías
* Carga de datos
* Pre-procesamiento de datos: gestión de tipos de datos, valores duplicados y nulos, gestión de características
* Análisis de datos: incluye series de tiempo, distribuciones y formatos diversos para analizar la evolución a través del tiempo de características
* Conclusiones

Entregables del Sprint 01:
* Project Charter: describe el contexto general del proyecto, e incluye:
    * Metodología de trabajo
    * Alcance y objetivos
    * KPIs
    * Esquema de trabajo
    * Stack tecnológico
* EDA (Exploratory Data Analysis) o Análisis exploratorio de datos

Estructura de datos simplificada
![Texto alternativo](DataStructureSimple.png)

Estrucura de datos completa
![Texto alternativo](DataStructureComplete.png)



## Resumen de Sprint 02
Esta etapa consiste en la instalación, adecuación y desarrollo de una infraestructura de datos en un Data Warehouse. El proceso de ETL se presenta a través de un pipeline que contempla la carga incremental de datos. La plataforma seleccionada para esta tarea es Google Cloud Platform, en donde se utilizan los servicios de:
* Cloud storage
* Conexión a APIs
* Big queries
* Data analysis

Se presenta una estructura propuesta de entidad-relaciones entre las tablas utilizadas, y asimismo un diccionario de datos. Estos formatos están sujetos a cambios si así lo requieren en el futuro, y constituyen la base para el producto final de dashboard a presentar en el Sprint 3.
  
Luego se presenta la estructura del datawarehouse en donde se muestran los procesamientos y las fases de estos procesos. Estos diagramas consisten en el centro del sprint en cuestión, pues proporcionana los detalles para el entendimiento del flujo de información. 

<img width="796" alt="image" src="https://github.com/Batxa/DS_ProjectFinal/assets/17538681/cc498414-363f-4e5c-9015-02b3e86a1bad">

Finalmente se presenta un MVP para el producto de machine learning. El mismo consiste en un sistema de recomendación de restaurantes basado en las reseñas de los usuarios. Las herramientas para su desarrollo han sido Scikit-Learn, NLKT, Fuzzywuzzy, y Streamlit. El sistema contempla el ingreso de inputs por parte del cliente y la obtención de outputs. A través de la selección de sus preferencias, el cliente obtiene una recomendación de restaurantes basado en las mismas.

Entregables del Sprint 02:
* Reporte de Data Engineering

## Resumen de Sprint 03


