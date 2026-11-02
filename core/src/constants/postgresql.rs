pub const DEFAULT_SCHEMA: &str = "public";

/// Base tables, optionally filtered by schema ($1 may be NULL).
pub const POSTGRESQL_TABLES_QUERY: &str = "
SELECT table_schema::text, table_name::text
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
  AND ($1::text IS NULL OR table_schema = $1::text)
ORDER BY table_schema, table_name";

pub const POSTGRESQL_COLUMNS_QUERY: &str = "
SELECT c.column_name::text, c.data_type::text, (c.is_nullable = 'YES') AS is_nullable,
       EXISTS (
           SELECT 1 FROM pg_constraint pc
           JOIN pg_class t ON t.oid = pc.conrelid
           JOIN pg_namespace n ON n.oid = t.relnamespace
           JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (pc.conkey)
           WHERE pc.contype = 'p' AND n.nspname = c.table_schema AND t.relname = c.table_name
             AND a.attname = c.column_name
       ) AS is_primary_key
FROM information_schema.columns c
WHERE c.table_schema = $1 AND c.table_name = $2
ORDER BY c.ordinal_position";

pub const POSTGRESQL_FOREIGN_KEYS_QUERY: &str = "
SELECT pc.conname::text, rn.nspname::text, rt.relname::text,
       ARRAY(SELECT a.attname::text FROM unnest(pc.conkey) WITH ORDINALITY k(n, o)
             JOIN pg_attribute a ON a.attrelid = pc.conrelid AND a.attnum = k.n ORDER BY k.o),
       ARRAY(SELECT a.attname::text FROM unnest(pc.confkey) WITH ORDINALITY k(n, o)
             JOIN pg_attribute a ON a.attrelid = pc.confrelid AND a.attnum = k.n ORDER BY k.o)
FROM pg_constraint pc
JOIN pg_class t ON t.oid = pc.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_class rt ON rt.oid = pc.confrelid
JOIN pg_namespace rn ON rn.oid = rt.relnamespace
WHERE pc.contype = 'f' AND n.nspname = $1 AND t.relname = $2
ORDER BY pc.conname";

pub const POSTGRESQL_INDEXES_QUERY: &str = "
SELECT i.relname::text, ix.indisunique,
       ARRAY(SELECT a.attname::text FROM unnest(ix.indkey::int2[]) WITH ORDINALITY k(n, o)
             JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.n ORDER BY k.o)
FROM pg_index ix
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_class i ON i.oid = ix.indexrelid
WHERE n.nspname = $1 AND t.relname = $2
  AND NOT ix.indisprimary AND ix.indexprs IS NULL AND ix.indpred IS NULL
ORDER BY i.relname";
