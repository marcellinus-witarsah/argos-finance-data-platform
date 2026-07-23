# Install Image with Python Version 3.10.11
ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim AS spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gettext-base \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    software-properties-common \
    openjdk-17-jre-headless \
    build-essential \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM spark-base AS spark

# =======================================================================
# Create folder for the Hadoop and Spark folders
# =======================================================================
ARG SPARK_VERSION
# RUN echo $SPARK_VERSION
ENV SPARK_HOME="/root/spark"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# =======================================================================
# Install Standalone Spark
# =======================================================================
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz


ARG SPARK_SCALA_VERSION
ARG ICEBERG_VERSION
ARG POSTGRESQL_JAR_VERSION
ARG HADOOP_AWS_JAR_VERSION
ARG AWS_JAVA_SDK_BUNDLE_JAR_VERSION

FROM spark AS spark-deps
# =======================================================================
# Copy Necessary Files for Installation
# =======================================================================
# Common scripts
COPY ./scripts/ /tmp/scripts/

# Spark initialization scripts
COPY ./infrastructures/spark/scripts/ ./scripts/

# Spark Configuration files
COPY ./infrastructures/spark/conf /tmp/conf

# =======================================================================
# Install Spark Dependencies 
# =======================================================================
RUN chmod u+x ./scripts/dependency.sh
RUN ./scripts/dependency.sh

# =======================================================================
# Install Python Libraries 
# =======================================================================
ARG SPARK_VERSION
ARG SCALA_VERSION
ARG ICEBERG_VERSION
ARG POSTGRESQL_JAR_VERSION
ARG HADOOP_AWS_JAR_VERSION
ARG AWS_JAVA_SDK_BUNDLE_JAR_VERSION

FROM spark-deps AS spark-python
# Install python dependencies
COPY ./infrastructures/spark/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# =======================================================================
# Set Environment Variables for Spark 
# =======================================================================
# TODO: Set all the environment variables accordingly
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3

# =======================================================================
# Set The sbin and bin to be Executable
# =======================================================================
RUN chmod u+x ./sbin/* && \
    chmod u+x ./bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# =======================================================================
# Run init script
# =======================================================================
RUN chmod u+x ./scripts/init.sh
ENTRYPOINT [ "./scripts/init.sh" ]