FROM apache/airflow:2.10.2-python3.11

USER root

# Aktualizacja repozytoriów
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    cmake \
    libboost-all-dev \
    build-essential \
    python3-dev \
    openjdk-17-jdk \
    && apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Instalacja Apache Airflow i providerów
RUN pip install --upgrade pip setuptools wheel && \
    pip install apache-airflow==2.10.2 && \
    pip install apache-airflow-providers-openlineage==1.12.2 && \
    pip install apache-airflow-providers-google==10.24.0 && \
    pip install apache-airflow-providers-apache-spark==4.11.1

# Instalacja PySpark
RUN pip install pyspark==3.4.2

# Instalacja kompatybilnej wersji SpaCy i SciSpaCy
RUN pip install spacy==3.7.3 scispacy==0.5.5

# Pobieranie modelu SpaCy
RUN python -m spacy download en_core_web_lg

# Instalacja starszej wersji nmslib
RUN pip install "nmslib<2.1"

# Ustawienie użytkownika z powrotem na airflow
USER airflow


###########################################
#FROM apache/airflow:2.10.2-python3.10

#USER root

# Instalacja zależności systemowych

# Aktualizacja listy pakietów
#RUN apt-get update

# Instalacja gcc
#RUN apt-get install -y gcc

# Instalacja g++
#RUN apt-get install -y g++

# Instalacja cmake
#RUN apt-get install -y cmake

# Instalacja libboost-all-dev
#RUN apt-get install -y libboost-all-dev

# Instalacja build-essential
#RUN apt-get install -y build-essential

# Instalacja python3-dev
#RUN apt-get install -y python3-dev

# Instalacja openjdk-17-jdk
#RUN apt-get install -y openjdk-17-jdk

# Czyszczenie cache apt
#RUN apt-get clean


# Ustawienie JAVA_HOME
#ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

#USER airflow

# Aktualizacja pip i podstawowych narzędzi
#RUN pip install --upgrade pip setuptools wheel

# Instalacja Apache Airflow i providerów
#RUN pip install apache-airflow==2.10.2

#RUN pip install apache-airflow-providers-openlineage==1.12.2

#RUN pip install apache-airflow-providers-google==10.24.0

#RUN pip install apache-airflow-providers-apache-spark==4.11.1

# Instalacja PySpark
#RUN pip install pyspark==3.4.2

# Instalacja kompatybilnej wersji SpaCy i SciSpaCy
#RUN pip install spacy==3.7.3 scispacy==0.5.5

# Pobieranie modelu SpaCy
# Pobieranie modelu SciSpaCy
#RUN python -m spacy download en_core_web_lg


# Instalacja starszej wersji nmslib
#RUN pip install "nmslib<2.1"


######################################

#FROM apache/airflow:2.10.2-python3.11

#USER root

#RUN apt-get update

#RUN apt-get install -y gcc python3-dev

#RUN apt-get install -y openjdk-17-jdk

#RUN apt-get clean

# Set JAVA_HOME environment variable
#ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

#USER airflow


#RUN pip install apache-airflow==2.10.2

#RUN pip install apache-airflow-providers-openlineage==1.12.2

#RUN pip install apache-airflow-providers-google==10.24.0

#RUN pip install apache-airflow-providers-apache-spark==4.11.1

#RUN pip install pyspark==3.4.2


