# Usamos la versión que quieras de Apache Airflow
FROM apache/airflow:2.10.4

# usuario 'airflow'
USER airflow

# Copiamos archivos de requerimientos
COPY requirements.txt /tmp/requirements.txt

# Instalamos paquetes
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Cambiamos a root temporalmente para instalar paquetes
USER root

# Instalamos Java (OpenJDK) en el contenedor
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gdal-bin \
        libgdal-dev \
        openjdk-17-jre \
        python3-gdal && \
    apt-get clean

# volvemos al usuario 'airflow'
USER airflow

# Creamos la carpeta /app (opcional) para copiar nuestro proyecto
WORKDIR /app

# Copiamos nuestro código fuente (DAGs y tests) dentro del contenedor
COPY . /app