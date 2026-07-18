#!/bin/bash

# =======================================================================
# Create metalake in Gravitino
# =======================================================================
response=$(curl http://gravitino:8090/api/metalakes/$GRAVITINO_METALAKE_NAME)
if echo "$response" | grep -q "\"code\":0"; then
  true
else
  response=$(curl -X POST -H "Content-Type: application/json" -d '{"name":"'$GRAVITINO_METALAKE_NAME'","comment":"comment","properties":{}}' http://gravitino:8090/api/metalakes)
  if echo "$response" | grep -q "\"code\":0"; then
    true # Placeholder, do nothing
  else
    echo "Metalake $GRAVITINO_METALAKE_NAME create failed"
    exit 1
  fi
fi

# =======================================================================
# Create data catalog in Gravitino
# =======================================================================
response=$(curl http://gravitino:8090/api/metalakes/$GRAVITINO_METALAKE_NAME/catalogs/$GRAVITINO_CATALOG_NAME)
if echo "$response" | grep -q "\"code\":0"; then
  true
else
  # Create Iceberg catalog for experience Gravitino service
  response=$(curl -X POST -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" -d '{ "name":"'$GRAVITINO_CATALOG_NAME'", "type":"RELATIONAL", "provider":"lakehouse-iceberg", "comment":"comment", "properties":{ "uri":"jdbc:postgresql://metastore-db-postgres:5432/catalog_metastore_db", "catalog-backend":"jdbc", "warehouse":"s3a://iceberg/warehouse", "jdbc-user":"postgres", "jdbc-password":"postgres", "jdbc-driver":"org.postgresql.Driver"} }' http://gravitino:8090/api/metalakes/$GRAVITINO_METALAKE_NAME/catalogs)
  if echo "$response" | grep -q "\"code\":0"; then
    true # Placeholder, do nothing
  else
    echo "create $GRAVITINO_CATALOG_NAME failed"
    exit 1
  fi
fi