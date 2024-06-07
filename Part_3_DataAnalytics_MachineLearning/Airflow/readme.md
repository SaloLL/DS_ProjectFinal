# Airflow con Docker

## Introducción

Este repositorio contiene todos los archivos necesarios para configurar y ejecutar Apache Airflow usando Docker.

## ¿Qué es Docker?

Docker es una plataforma para desarrollar, enviar y ejecutar aplicaciones dentro de contenedores. Los contenedores permiten a un desarrollador empaquetar una aplicación con todas las partes que necesita, como bibliotecas y otras dependencias, y enviarla como un único paquete. Al hacerlo, la aplicación se ejecutará en cualquier otro sistema habilitado para Docker, independientemente de cualquier configuración personalizada que pueda tener esa máquina que difiera de la máquina utilizada para escribir y probar el código.

Para más información, visita el [sitio web de Docker](https://www.docker.com/).

## ¿Qué es Apache Airflow?

Apache Airflow es una plataforma de código abierto para crear, programar y monitorear flujos de trabajo de manera programática. Airflow permite definir flujos de trabajo como código y rastrear el estado de tus trabajos. Soporta un conjunto rico de características para manejar programación compleja y dependencias de tareas, lo que lo convierte en una excelente herramienta para orquestar pipelines de datos.

Para más información, visita el [sitio web de Apache Airflow](https://airflow.apache.org/).

## Ejecutar Airflow con Docker

El directorio actual contiene todos los archivos necesarios para ejecutar Apache Airflow en Docker en cualquier máquina. Esto incluye un archivo `docker-compose.yml` y un `Dockerfile` con las bibliotecas de Python necesarias para ejecutar los DAGs.

### Prerrequisitos

Asegúrate de tener Docker instalado en tu máquina. Puedes descargar e instalar Docker desde el [sitio web oficial de Docker](https://docs.docker.com/get-docker/).

### Archivos en este Repositorio

- **Dockerfile**: Este archivo contiene las instrucciones para construir la imagen Docker para Airflow.
- **docker-compose.yml**: Este archivo define los servicios, redes y volúmenes para ejecutar Airflow en Docker.
- **requirements.txt**: Este archivo lista paquetes adicionales de Python para instalar en el contenedor de Airflow.
- **dags/**: Este directorio contiene tus DAGs (Grafos Acíclicos Dirigidos) de Airflow que definen tus flujos de trabajo.

### Pasos para Ejecutar

1. **Clonar el repositorio**:
    ```sh
    git clone https://github.com/Batxa/DS_ProjectFinal.git
    cd DS_ProjectFinal
    ```

2. **Construir la imagen Docker**:
    ```sh
    docker-compose build
    ```

3. **Inicializar la base de datos de Airflow**:
    ```sh
    docker-compose up airflow-init
    ```

4. **Iniciar Airflow**:
    ```sh
    docker-compose up
    ```

5. **Acceder a la interfaz web de Airflow**:
    Abre tu navegador web y ve a `http://localhost:8080`. Utiliza las credenciales por defecto para iniciar sesión:
    - Usuario: `airflow`
    - Contraseña: `airflow`

### Detener Airflow

Para detener los servicios de Airflow, presiona `Ctrl+C` en la terminal donde los servicios están ejecutándose. También puedes usar el siguiente comando para detener y eliminar todos los contenedores:
```sh
docker-compose down
```