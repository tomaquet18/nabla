-- nabla bootstrap: catalog tables and the direct-write guard.
-- Runs first in the extension script (extension_sql_file! ... bootstrap).

CREATE SCHEMA IF NOT EXISTS nabla;
-- Shadow copies of base tables used by join views (see README "Joins and shadow tables").
CREATE SCHEMA IF NOT EXISTS nabla_shadow;
-- Storage tables behind the user-facing VIEWs (nabla_store.v<view id>).
CREATE SCHEMA IF NOT EXISTS nabla_store;

CREATE TABLE nabla.views (
  id            serial PRIMARY KEY,
  name          text NOT NULL UNIQUE,           -- canonical schema-qualified name of the VIEW (quoted where needed)
  relid         oid,                            -- oid of the VIEW; NULL until built
  definition    text NOT NULL,
  shape         text NOT NULL CHECK (shape IN ('projection', 'aggregate')),
  spec          jsonb NOT NULL,                  -- parsed structure, see src/definition.rs
  frontier_lsn  pg_lsn NOT NULL,                 -- the view equals its query at this WAL position
  epoch         int NOT NULL DEFAULT 1,          -- bumped by refresh; subscribers must resync
  status        text NOT NULL DEFAULT 'initializing'
                CHECK (status IN ('initializing', 'refreshing', 'live', 'stale', 'failed')),
  last_seq      bigint NOT NULL DEFAULT 0,       -- last delta sequence number handed out
  -- Failure isolation (see README "Failure isolation"). Added in v0.1 before
  -- any release, so no upgrade script migrates older catalogs.
  apply_failures int NOT NULL DEFAULT 0,          -- consecutive failed applies of the pending transaction
  last_error    text,
  last_error_at timestamptz,
  stale_reason  text,                             -- why status became 'stale'
  populated_at  timestamptz,                      -- first successful build (shadow references exist)
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
  columns       text[] NOT NULL DEFAULT '{}',   -- active column set: primary key + columns some view uses
  pk_columns    text[] NOT NULL DEFAULT '{}',
  column_types  text[] NOT NULL DEFAULT '{}',   -- format_type() text, parallel to columns
  failed        bool NOT NULL DEFAULT false,    -- maintenance stopped; refresh rebuilds
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

CREATE INDEX ON nabla.views (relid);

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
      USING HINT = 'The table is maintained by the nabla worker. Change the base table instead.',
            ERRCODE = 'NB005';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$$;

-- Forget a view: release its shadow references (taken when its first build
-- committed), drop shadows nobody uses any more, and remove the catalog row.
-- Idempotent; safe when the tables are already gone.
CREATE FUNCTION nabla.forget_view(view_id int) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  v record;
  r record;
  s record;
BEGIN
  SELECT id, name, populated_at, jsonb_array_length(spec->'relations') > 1 AS is_join,
         ARRAY(SELECT relid FROM nabla.view_relations vr WHERE vr.view_id = views.id) AS relids
    INTO v FROM nabla.views WHERE id = view_id;
  IF NOT FOUND THEN
    RETURN;
  END IF;
  -- Remove the catalog row first: the DROPs below re-enter this function
  -- through the sql_drop trigger, and a second pass must find nothing.
  DELETE FROM nabla.views WHERE id = v.id;
  IF v.is_join AND v.populated_at IS NOT NULL THEN
    FOR r IN SELECT relid FROM unnest(v.relids) AS relid LOOP
      UPDATE nabla.shadows SET refcount = refcount - 1 WHERE relid = r.relid;
      FOR s IN SELECT table_name FROM nabla.shadows WHERE relid = r.relid AND refcount <= 0 LOOP
        IF to_regclass(s.table_name) IS NOT NULL THEN
          EXECUTE 'DROP TABLE ' || s.table_name;
        END IF;
        DELETE FROM nabla.shadows WHERE table_name = s.table_name;
      END LOOP;
    END LOOP;
  END IF;
  -- The VIEW and its storage, whichever still exists (DDL from within the
  -- sql_drop trigger is allowed; existence is checked to avoid NOTICEs).
  IF to_regclass(v.name) IS NOT NULL THEN
    EXECUTE 'DROP VIEW ' || v.name;
  END IF;
  IF to_regclass('nabla_store.v' || v.id) IS NOT NULL THEN
    EXECUTE 'DROP TABLE nabla_store.v' || v.id;
  END IF;
END
$$;

-- Remove from the publication every table no view and no shadow needs.
CREATE FUNCTION nabla.prune_publication() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  t record;
BEGIN
  -- Runs inside the sql_drop trigger too: ALTER PUBLICATION ... DROP TABLE
  -- itself drops a publication-relation object, so re-check membership on
  -- every iteration and ignore a table another invocation already removed.
  FOR t IN
    SELECT pt.schemaname, pt.tablename
    FROM pg_catalog.pg_publication_tables pt
    WHERE pt.pubname = 'nabla'
      AND NOT EXISTS (
        SELECT 1 FROM nabla.view_relations vr
        JOIN pg_catalog.pg_class c ON c.oid = vr.relid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = pt.schemaname AND c.relname = pt.tablename)
      AND NOT EXISTS (
        SELECT 1 FROM nabla.shadows s
        JOIN pg_catalog.pg_class c ON c.oid = s.relid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = pt.schemaname AND c.relname = pt.tablename)
  LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_publication_tables pt
               WHERE pt.pubname = 'nabla' AND pt.schemaname = t.schemaname AND pt.tablename = t.tablename) THEN
      BEGIN
        EXECUTE format('ALTER PUBLICATION nabla DROP TABLE %I.%I', t.schemaname, t.tablename);
      EXCEPTION WHEN undefined_object THEN
        NULL;
      END;
    END IF;
  END LOOP;
END
$$;

-- Keep the catalog consistent with whatever DROP removed: a view table
-- (dropped directly or by CASCADE), a shadow table, or a base table. View and
-- shadow tables depend on their base tables in pg_depend, so DROP TABLE base
-- requires CASCADE and takes them along; this trigger then forgets them.
-- Only the names carried by pg_event_trigger_dropped_objects() are used.
CREATE FUNCTION nabla.on_sql_drop() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
  obj record;
  v record;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() WHERE object_type IN ('table', 'view')) THEN
    RETURN;
  END IF;
  FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() WHERE object_type IN ('table', 'view') LOOP
    -- The user-facing VIEW of a nabla view.
    FOR v IN SELECT id FROM nabla.views WHERE relid = obj.objid LOOP
      PERFORM nabla.forget_view(v.id);
    END LOOP;
    -- A storage table (nabla_store.v<id>).
    IF obj.schema_name = 'nabla_store' AND obj.object_name ~ '^v[0-9]+$' THEN
      PERFORM nabla.forget_view(substr(obj.object_name, 2)::int);
    END IF;
    DELETE FROM nabla.shadows WHERE table_name = obj.schema_name || '.' || obj.object_name;
    FOR v IN SELECT DISTINCT view_id AS id FROM nabla.view_relations WHERE relid = obj.objid LOOP
      PERFORM nabla.forget_view(v.id);
    END LOOP;
    DELETE FROM nabla.shadows WHERE relid = obj.objid;
  END LOOP;
  PERFORM nabla.prune_publication();
END
$$;

CREATE EVENT TRIGGER nabla_sql_drop ON sql_drop EXECUTE FUNCTION nabla.on_sql_drop();
