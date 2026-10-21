# Fork known issues (dapperlabs/ingestr)

Tracked here because Issues are disabled on this fork.

## pg destination: mixed-case `--dest-table` silently disables the generated-column skip

Found 2026-10-20 during the atlas staging fidelity proof. With a quoted,
capitalised target (`--dest-table Breaks`), the copy fails with
`cannot insert a non-DEFAULT value into column "active_host_key"` — the
skip silently no-ops.

**Mechanism:** `resolveSchemaTable` (pkg/destination/postgres/postgres.go:870)
resolves an unqualified target via `to_regclass($1)` on the raw name. Unquoted
`Breaks` case-folds to `breaks`; no relation matches; the fallback reports
tableName `breaks`. `inspectGeneratedColumns` then queries `relname='breaks'`
against the relation actually named `Breaks`, returns empty, and
`dropGeneratedColumns` drops nothing — while the write side quotes correctly
and hits the real table. Skip and write disagree about which table exists.

**Workaround:** use lowercase, schema-qualified targets (`public.breaks`) —
what the atlas gorm/AutoMigrate schema produces anyway.

**Fix direction:** pass the parsed/quoted identifier to `to_regclass` so the
resolution and the write agree.

## Migration conventions discovered by the fidelity proof

- Destination columns are snake_cased naively: `DistributionIDs` becomes
  `distribution_i_ds` (not `distribution_ids`). The pg DDL must use ingestr's
  convention or carry explicit mappings.
- Destination tables must pre-exist; `replace`/`delete+insert` truncate, they
  do not create. Schema comes from the app models, per the migration plan.
- ingestr appends `_ingestr_loaded_at` / `_ingestr_run_id` columns to the
  destination insert list.
