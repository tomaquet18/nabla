-- nabla bootstrap: catalog tables and the direct-write guard.
-- Runs first in the extension script (extension_sql_file! ... bootstrap).

CREATE SCHEMA IF NOT EXISTS nabla;
-- Shadow copies of base tables used by join views (see README "Joins and shadow tables").
CREATE SCHEMA IF NOT EXISTS nabla_shadow;

CREATE TABLE nabla.views (
  id            serial PRIMARY KEY,
  name          text NOT NULL UNIQUE,           -- schema-qualified name of the view table
  base_table    regclass NOT NULL,
  definition    text NOT NULL,
  shape         text NOT NULL CHECK (shape IN ('projection', 'aggregate')),
  spec          jsonb NOT NULL,                  -- parsed structure, see src/definition.rs
  frontier_lsn  pg_lsn NOT NULL,                 -- the view equals its query at this WAL position
  epoch         int NOT NULL DEFAULT 1,          -- bumped by refresh; subscribers must resync
  status        text NOT NULL DEFAULT 'live' CHECK (status IN ('live', 'stale')),
  last_seq      bigint NOT NULL DEFAULT 0,       -- last delta sequence number handed out
  resync_seq    bigint NOT NULL DEFAULT 0,       -- cursors below this must resync (set by refresh)
  -- Failure isolation (see README "Failure isolation"). Added in v0.1 before
  -- any release, so no upgrade script migrates older catalogs.
  apply_failures int NOT NULL DEFAULT 0,          -- consecutive failed applies of the pending transaction
  last_error    text,
  last_error_at timestamptz,
  stale_reason  text,                             -- why status became 'stale'
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE nabla.deltas (
  view_id  int NOT NULL REFERENCES nabla.views(id) ON DELETE CASCADE,
  seq      bigint NOT NULL,        -- per-view, contiguous, monotonic
  lsn      pg_lsn NOT NULL,        -- end LSN of the source transaction's commit record
  xid      bigint,                 -- source transaction id, for grouping on the client
  op       "char" NOT NULL,        -- 'I' insert, 'D' delete (an update is D then I)
  row      jsonb NOT NULL,
  PRIMARY KEY (view_id, seq)
);

-- Shadow tables: one per base table used by any join view. The shadow itself
-- (nabla_shadow.t<oid>) is not dumped; nabla.refresh rebuilds it after a restore.
CREATE TABLE nabla.shadows (
  relid         oid PRIMARY KEY,
  table_name    text NOT NULL,                  -- nabla_shadow.t<oid>
  frontier_lsn  pg_lsn NOT NULL,
  refcount      int NOT NULL,                   -- join views using it
  stale_reason  text,                           -- set when maintenance failed; rebuilt by refresh
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- Which base tables each view reads (rti = position in the definition's range table).
CREATE TABLE nabla.view_relations (
  view_id  int NOT NULL REFERENCES nabla.views(id) ON DELETE CASCADE,
  relid    oid NOT NULL,
  rti      int NOT NULL,
  PRIMARY KEY (view_id, relid)
);

SELECT pg_catalog.pg_extension_config_dump('nabla.views', '');
SELECT pg_catalog.pg_extension_config_dump('nabla.shadows', '');
SELECT pg_catalog.pg_extension_config_dump('nabla.view_relations', '');
SELECT pg_catalog.pg_extension_config_dump('nabla.views_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('nabla.deltas', '');

-- View tables are maintained only by the nabla worker. The worker (and
-- nabla.refresh) set the session GUC nabla.internal_write = on for the
-- duration of their transaction; every other writer is rejected.
CREATE FUNCTION nabla.guard_view() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF coalesce(current_setting('nabla.internal_write', true), 'off') <> 'on' THEN
    RAISE EXCEPTION 'nabla: cannot modify a nabla view directly'
      USING HINT = 'The view is maintained by the nabla worker. Change the base table instead.',
            ERRCODE = 'insufficient_privilege';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$$;
