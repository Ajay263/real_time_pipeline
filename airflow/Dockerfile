FROM apache/airflow:2.7.1

USER root

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

# Install PySpark and other required packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set PYSPARK_PYTHON environment variable
ENV PYSPARK_PYTHON /usr/local/bin/python
RUN chmod -R 755 /opt/airflow/dags/src
