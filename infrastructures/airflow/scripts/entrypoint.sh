#!/bin/bash
set -e

# =======================================================================
# Render Spark configuration templates with the runtime environment
# (e.g. MINIO_ACCESS_KEY, MINIO_SECRET_KEY, GRAVITINO_CATALOG_NAME),
# which are only available at container start, not at image build time.
# Removing any stale rendered file first means a leftover owned by a
# different user (e.g. from a prior root run) can't block the rewrite —
# deleting only needs write access to the directory, not the file itself.
# =======================================================================
rm -f ${SPARK_HOME}/conf/spark-defaults.conf ${SPARK_HOME}/conf/core-site.xml
envsubst < /tmp/conf/spark-defaults.conf.template > ${SPARK_HOME}/conf/spark-defaults.conf
envsubst < /tmp/conf/core-site.xml.template > ${SPARK_HOME}/conf/core-site.xml

# =======================================================================
# Hand off to Airflow's own entrypoint so the requested command
# (celery worker, scheduler, api-server, ...) still runs normally.
# =======================================================================
exec /entrypoint "$@"
