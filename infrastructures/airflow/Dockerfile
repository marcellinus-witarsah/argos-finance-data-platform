# Install Image with Python Version 3.10.11
FROM apache/airflow:3.3.0 AS spark-base

USER root

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
ENV SPARK_HOME="/opt/spark"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# =======================================================================
# Install Standalone Spark
# =======================================================================
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf ${SPARK_HOME}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

FROM spark AS spark-deps
# =======================================================================
# Copy Necessary Files for Installation
# =======================================================================
# Common scripts
COPY ./scripts/ /tmp/scripts/

# Spark initialization scripts
COPY ./infrastructures/orchestration/airflow/scripts/ ./scripts/

# Spark Configuration files
COPY ./infrastructures/orchestration/airflow/conf /tmp/conf

# =======================================================================
# Install Spark Dependencies
# =======================================================================
ARG SPARK_SCALA_VERSION
ARG ICEBERG_VERSION
ARG POSTGRESQL_JAR_VERSION
ARG HADOOP_AWS_JAR_VERSION
ARG AWS_JAVA_SDK_BUNDLE_JAR_VERSION

RUN chmod u+x ./scripts/dependency.sh
RUN ./scripts/dependency.sh

FROM spark-deps AS spark-python
# Install python dependencies
COPY ./infrastructures/orchestration/airflow/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# =======================================================================
# Set Environment Variables for Spark 
# =======================================================================
# TODO: Set all the environment variables accordingly
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
# Specify Python executables the worker nodes need to use for run PySpark code
ENV PYSPARK_PYTHON=python3

# =======================================================================
# Set The sbin and bin to be Executable
# =======================================================================
RUN chmod u+x ./sbin/* && \
    chmod u+x ./bin/*
# PySpark is uniquely stored inside $SPARK_HOME/python/ directory because
# it is packaged inside core Apache Spark distribution
ENV PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/apps/

# =======================================================================
# Install JARs into $SPARK_HOME/jars (build-time, no runtime env needed)
# =======================================================================
ARG SPARK_SCALA_VERSION
ARG ICEBERG_VERSION
ARG HADOOP_AWS_JAR_VERSION
ARG AWS_JAVA_SDK_BUNDLE_JAR_VERSION

RUN chmod u+x ./scripts/init.sh
RUN ./scripts/init.sh

# =======================================================================
# Create the Spark event-log dir in the image (empty) so that when
# docker-compose mounts a fresh named volume over it, Docker's volume
# "copy-up" initializes the volume from this directory - inheriting its
# ownership/permissions instead of defaulting to root:root.
# =======================================================================
RUN mkdir -p ${SPARK_HOME}/spark-events

# # =======================================================================
# # Containers run as a non-root UID with GID 0 (see docker-compose
# # "user: ${AIRFLOW_UID:-50000}:0"), so force group ownership to 0 before
# # mirroring owner perms onto the group. The Spark tarball's extracted
# # files can end up owned by a build-time UID/GID that isn't 0 (varies by
# # builder), so chmod g=u alone isn't enough - the group itself must be
# # gid 0 for the runtime entrypoint to be able to write ${SPARK_HOME}/conf/*
# # =======================================================================
RUN chgrp -R 0 ${SPARK_HOME} && chmod -R g=u ${SPARK_HOME}

# =======================================================================
# Runtime entrypoint: renders Spark config templates with the actual
# runtime environment, then hands off to Airflow's own entrypoint
# =======================================================================
RUN chmod u+x ./scripts/entrypoint.sh

# =======================================================================
# Drop back to the non-root airflow user (UID 50000, GID 0) so the image's
# own default matches docker-compose's "user: ${AIRFLOW_UID:-50000}:0".
# Without this, any container/exec started without an explicit -u flag
# runs as root and leaves root-owned files under ${SPARK_HOME}/conf,
# which then permanently block later non-root runs from rewriting them.
# =======================================================================
USER airflow

ENTRYPOINT [ "./scripts/entrypoint.sh" ]