# =======================================================================
# Build layer image from apache/gravitino base image
# =======================================================================
ARG GRAVITINO_IMAGE_TAG
FROM apache/gravitino:${GRAVITINO_IMAGE_TAG} AS gravitino-base
RUN apt-get update \
    && apt-get install gettext-base -y

# =======================================================================
# Build layer image from gravitino-base image
# =======================================================================
ARG HADOOP_AWS_JAR_VERSION
ARG AWS_JAVA_SDK_BUNDLE_JAR_VERSION
FROM gravitino-base AS gravitino

# Copy configuration files
COPY ./infrastructures/data_lakehouse/gravitino/conf /tmp/conf

# Copy Gravitino initialization scripts
COPY ./infrastructures/data_lakehouse/gravitino/scripts/ ./scripts/

# Copy common initialization scripts
COPY ./scripts/ /tmp/scripts/

# Install Gravitino Dependencies
RUN chmod u+x ./scripts/dependency.sh
RUN ./scripts/dependency.sh

# Run Gravitino Initialization Script
RUN chmod u+x ./scripts/init.sh
ENTRYPOINT [ "./scripts/init.sh" ]