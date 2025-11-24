#!/bin/sh
set -e

export HADOOP_HOME=/opt/hadoop-3.2.0
export JAVA_HOME=/usr/local/openjdk-8

# Supported values: "postgres" or "mysql"
export METASTORE_DB_TYPE=${METASTORE_DB_TYPE:-postgres}
export METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-localhost}
export METASTORE_DB_PORT=${METASTORE_DB_PORT:-5432}
export METASTORE_DB_NAME=${METASTORE_DB_NAME:-metastore}

if [ "$METASTORE_DB_TYPE" = "postgres" ]; then
  export METASTORE_DB_JDBC="jdbc:postgresql://${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT}/${METASTORE_DB_NAME}"
  export METASTORE_DB_DRIVER="postgres"
elif [ "$METASTORE_DB_TYPE" = "mysql" ]; then
  export METASTORE_DB_DRIVER="mysql"
  export METASTORE_DB_JDBC="jdbc:mysql://${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT}/${METASTORE_DB_NAME}"
else
  echo "❌ Unknown METASTORE_DB_TYPE: $METASTORE_DB_TYPE"
  exit 1
fi

echo "⏳ Waiting for $METASTORE_DB_TYPE database on ${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT} ..."

while ! nc -z ${METASTORE_DB_HOSTNAME} ${METASTORE_DB_PORT}; do
  sleep 1
done

echo "✅ Database is available"

echo "🔧 Initializing Hive Metastore schema for ${METASTORE_DB_TYPE}..."

schematool_cmd="/opt/apache-hive-metastore-3.0.0-bin/bin/schematool \
  -initSchema \
  -dbType ${METASTORE_DB_DRIVER} \
  -userName ${METASTORE_DB_USER} \
  -passWord ${METASTORE_DB_PASSWORD} \
  -url \"${METASTORE_DB_JDBC}\""

echo "Running: $schematool_cmd"

# Allow schema to already exist (common)
sh -c "$schematool_cmd" || echo "Schema already initialized"

echo "🚀 Starting Hive Metastore service..."
exec /opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
