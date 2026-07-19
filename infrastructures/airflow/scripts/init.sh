#!/bin/bash

# =======================================================================
# Copy all of the JAR files to their respective folders
# Build-time only: these paths/versions come from Dockerfile ARGs.
# =======================================================================
cp ./packages/iceberg-spark-runtime-${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/iceberg-spark-runtime-${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar
cp ./packages/hadoop-aws-${HADOOP_AWS_JAR_VERSION}.jar /opt/spark/jars/hadoop-aws-${HADOOP_AWS_JAR_VERSION}.jar
cp ./packages/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_JAR_VERSION}.jar /opt/spark/jars/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_JAR_VERSION}.jar
