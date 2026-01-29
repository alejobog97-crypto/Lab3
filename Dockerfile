# 1. Empezamos con un Linux mínimo con Python
FROM python:3.11-slim

# 2. Instalar Java (REQUISITO CRÍTICO)
# Spark corre sobre la JVM (Scala). PySpark necesita Java para funcionar.
RUN apt-get update && \
    apt-get install -y default-jdk procps curl && \
    apt-get clean

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 3. Configurar Variables de Entorno
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 4. Descargar e instalar Binarios de Spark
# Bajamos Spark compilado para Hadoop 3
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 5. Instalar librerías de Python
# delta-spark: Permite usar el formato Delta Lake
# jupyterlab: Nuestro entorno de desarrollo (Driver)
RUN pip install pyspark==${SPARK_VERSION} delta-spark==3.0.0 pandas jupyterlab

WORKDIR /app

# Por defecto, el contenedor intentará lanzar Jupyter.
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--NotebookApp.token=''"]
