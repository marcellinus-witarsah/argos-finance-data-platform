#!/bin/bash

# =======================================================================
# Copy all of the JAR files to their respective folders
# =======================================================================
cp ./packages/iceberg-spark-runtime-${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar /root/spark/jars/iceberg-spark-runtime-${SPARK_SCALA_VERSION}-${ICEBERG_VERSION}.jar
cp ./packages/hadoop-aws-${HADOOP_AWS_JAR_VERSION}.jar /root/spark/jars/hadoop-aws-${HADOOP_AWS_JAR_VERSION}.jar
cp ./packages/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_JAR_VERSION}.jar /root/spark/jars/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_JAR_VERSION}.jar

# =======================================================================
# Copy all of the configuration in their respective folders
# =======================================================================
envsubst < /tmp/conf/spark-defaults.conf.template > ${SPARK_HOME}/conf/spark-defaults.conf
envsubst < /tmp/conf/core-site.xml.template > ${SPARK_HOME}/conf/core-site.xml
