#!/bin/bash
set -e

echo "Starting database initialization script..."

# Connection parameters
DB_USER=${POSTGRES_USER:-user}
DB_NAME=${POSTGRES_DB:-exports_db}

# Check if table exists and has 10,000,000 rows
TABLE_EXISTS=$(psql -t -A -U "$DB_USER" -d "$DB_NAME" -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'records');")

if [ "$TABLE_EXISTS" = "t" ]; then
  COUNT=$(psql -t -A -U "$DB_USER" -d "$DB_NAME" -c "SELECT COUNT(*) FROM records;")
  if [ "$COUNT" -eq 10000000 ]; then
    echo "Database is already fully seeded with 10,000,000 records. Skipping seeding."
    exit 0
  fi
fi

echo "Database records count is not 10,000,000. Re-seeding database..."

psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname "$DB_NAME" <<-EOSQL
  -- Drop if exists
  DROP TABLE IF EXISTS records CASCADE;
  DROP SEQUENCE IF EXISTS records_id_seq CASCADE;

  -- Create table as UNLOGGED (no WAL, speeds up bulk insert)
  CREATE UNLOGGED TABLE records (
    id BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    name VARCHAR(255) NOT NULL,
    value DECIMAL(18, 4) NOT NULL,
    metadata JSONB NOT NULL
  );

  \echo 'Inserting 10,000,000 records streamingly...'
  -- Insert mock data using generate_series
  INSERT INTO records (id, name, value, metadata, created_at)
  SELECT 
    i AS id,
    'item_' || i AS name,
    (i * 0.01)::decimal(18,4) AS value,
    jsonb_build_object(
      'sku', 'SKU-' || i,
      'details', jsonb_build_object(
        'weight', (i % 100)::decimal(18,2) + 0.5,
        'dimensions', jsonb_build_object(
          'width', (i % 50)::decimal(18,2) + 1.0,
          'height', (i % 30)::decimal(18,2) + 1.0
        )
      )
    ) AS metadata,
    NOW() - (i || ' seconds')::interval AS created_at
  FROM generate_series(1, 10000000) AS i;

  \echo 'Creating primary key index...'
  -- Add primary key constraint (this will build the B-Tree index in one pass)
  ALTER TABLE records ADD PRIMARY KEY (id);

  -- Create and link sequence for auto-increment (BIGSERIAL behavior)
  CREATE SEQUENCE records_id_seq;
  ALTER TABLE records ALTER COLUMN id SET DEFAULT nextval('records_id_seq');
  ALTER SEQUENCE records_id_seq OWNED BY records.id;
  SELECT setval('records_id_seq', 10000000);

  \echo 'Setting table to LOGGED...'
  -- Convert to logged table
  ALTER TABLE records SET LOGGED;
EOSQL

echo "Database seeding completed successfully."
