-- Enable dblink extension
CREATE EXTENSION IF NOT EXISTS dblink;

-- Create databases only if they do not exist
DO
$do$
DECLARE
    conn_str text := 'host=127.0.0.1 port=5432 user=' || current_user;
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'articles'
   ) THEN
      PERFORM dblink_exec(conn_str, 'CREATE DATABASE articles');
   END IF;

   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'metastore'
   ) THEN
      PERFORM dblink_exec(conn_str, 'CREATE DATABASE metastore');
   END IF;
END;
$do$;

