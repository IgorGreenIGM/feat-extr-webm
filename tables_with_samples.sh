#!/bin/bash
set -e

export PGPASSWORD='Qosascomp20?'

HOST="aws-1-eu-west-1.pooler.supabase.com"
PORT=5432
USER="postgres.morskysepcapbiyfpklx"
DB="postgres"

OUT="schema_tables_plus_samples.sql"

# 2) Tables à exclure geometry
EXCLUDE_GEOM_TABLES=(
  "administrative_zones"
  "temp_arrondissements"
  "temp_departements"
)

# 3) Boucle sur les tables
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -At \
  -c "SELECT schemaname, tablename
      FROM pg_tables
      WHERE schemaname NOT IN ('pg_catalog','information_schema');" \
| while IFS='|' read -r schema table
do
  echo "" >> "$OUT"
  echo "-- Sample data for $schema.$table" >> "$OUT"

  # Vérifie si la table est dans la liste
  if [[ " ${EXCLUDE_GEOM_TABLES[*]} " == *" $table "* ]]; then

    # Génère dynamiquement la liste des colonnes sans geometry
    COLS=$(psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -At -c "
      SELECT string_agg(quote_ident(column_name), ', ')
      FROM information_schema.columns
      WHERE table_schema = '$schema'
        AND table_name = '$table'
        AND column_name <> 'geometry';
    ")

    psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "
      COPY (
        SELECT $COLS FROM \"$schema\".\"$table\" LIMIT 20
      ) TO STDOUT WITH CSV;
    " | sed "s/^/INSERT INTO \"$schema\".\"$table\" ($COLS) VALUES (/" \
      | sed "s/\$/);/" \
      >> "$OUT"

  else
    # Tables normales
    psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "
      COPY (
        SELECT * FROM \"$schema\".\"$table\" LIMIT 20
      ) TO STDOUT WITH CSV;
    " | sed "s/^/INSERT INTO \"$schema\".\"$table\" VALUES (/" \
      | sed "s/\$/);/" \
      >> "$OUT"
  fi
done
