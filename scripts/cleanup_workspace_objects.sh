#!/usr/bin/env bash
# cleanup_workspace_objects.sh — wipe UC tables/views/volumes; keep catalogs & schemas.
#
# Empties Unity Catalog schemas across the workspace without dropping catalogs or
# schemas. Landing volumes (e.g. bronze.landing) are deleted and must be recreated
# by bundle deploy or create_schemas_and_tables / land_sample_data notebooks.
#
# Auth: DATABRICKS_HOST + DATABRICKS_TOKEN, or `databricks auth login` / profile.
#
# Usage:
#   ./scripts/cleanup_workspace_objects.sh                  # dry-run (default)
#   ./scripts/cleanup_workspace_objects.sh --catalog actuarial --yes
#   ./scripts/cleanup_workspace_objects.sh --yes             # all non-protected catalogs
#
# Options:
#   --catalog NAME   Limit cleanup to one catalog
#   --yes            Actually delete (omit for dry-run)
#   -h, --help       Show this help

set -euo pipefail

PROTECTED_CATALOGS=("system" "samples" "hive_metastore" "main")
PROTECTED_SCHEMAS=("information_schema")

EXECUTE=0
CATALOG_FILTER=""

usage() {
  sed -n '2,18p' "$0" | sed -E 's/^# ?//'
}

log()  { printf '%s\n' "$*"; }
err()  { printf 'ERROR: %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

is_protected_catalog() {
  local name="$1"
  local p
  for p in "${PROTECTED_CATALOGS[@]}"; do
    [[ "$name" == "$p" ]] && return 0
  done
  return 1
}

is_protected_schema() {
  local name="$1"
  local p
  for p in "${PROTECTED_SCHEMAS[@]}"; do
    [[ "$name" == "$p" ]] && return 0
  done
  return 1
}

resolve_databricks() {
  # Prints the CLI path on stdout. Returns 1 on failure (caller must check;
  # macOS bash 3.2 does not honor set -e for failed command substitutions).
  if [[ -n "${DATABRICKS_BIN:-}" ]]; then
    if [[ ! -x "${DATABRICKS_BIN}" ]]; then
      err "DATABRICKS_BIN is set but not executable: ${DATABRICKS_BIN}"
      return 1
    fi
    printf '%s' "${DATABRICKS_BIN}"
    return 0
  fi
  # Prefer the modern CLI binary over the legacy PyPI shim when both exist.
  if [[ -x "${HOME}/.local/bin/databricks" ]]; then
    printf '%s' "${HOME}/.local/bin/databricks"
    return 0
  fi
  if command -v databricks >/dev/null 2>&1; then
    command -v databricks
    return 0
  fi
  err "databricks CLI not found. Install: https://docs.databricks.com/dev-tools/cli/install.html"
  return 1
}

dbx() {
  "${DBX[@]}" "$@"
}

# Normalize list JSON: accept a bare array or an object with a common key.
# Prints one name/full_name per line (empty if none).
jq_names() {
  local json="$1"
  local field="$2" # e.g. name or full_name
  local array_key="$3" # e.g. catalogs, schemas, tables, volumes
  printf '%s' "$json" | jq -r --arg f "$field" --arg k "$array_key" '
    if . == null then empty
    elif type == "array" then .[]? | .[$f] // empty
    elif type == "object" and (.[$k] | type) == "array" then .[$k][]? | .[$f] // empty
    else empty
    end
  '
}

# Emit "full_name\ttable_type" for tables list.
jq_tables() {
  local json="$1"
  printf '%s' "$json" | jq -r '
    if . == null then empty
    elif type == "array" then .[]?
    elif type == "object" and (.tables | type) == "array" then .tables[]?
    else empty
    end
    | select(.full_name != null)
    | "\(.full_name)\t\(.table_type // "TABLE")"
  '
}

jq_volume_names() {
  local json="$1"
  printf '%s' "$json" | jq -r '
    if . == null then empty
    elif type == "array" then .[]?
    elif type == "object" and (.volumes | type) == "array" then .volumes[]?
    else empty
    end
    | .full_name // empty
  '
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes)
      EXECUTE=1
      shift
      ;;
    --catalog)
      [[ $# -ge 2 ]] || die "--catalog requires a name"
      CATALOG_FILTER="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1 (try --help)"
      ;;
  esac
done

command -v jq >/dev/null 2>&1 || die "jq is required but not installed"

DBX_PATH="$(resolve_databricks)" || exit 1
DBX=("$DBX_PATH")
# Allow callers to pass through -p/--profile via DATABRICKS_CONFIG_PROFILE env (CLI reads it).

log "Using Databricks CLI: ${DBX_PATH}"
if ! dbx version >/dev/null 2>&1; then
  die "Unable to run Databricks CLI (${DBX_PATH})"
fi

# Auth / workspace connectivity check
if ! catalogs_json="$(dbx catalogs list -o json 2>&1)"; then
  die "Failed to list catalogs (check auth: DATABRICKS_HOST/TOKEN or databricks auth login). Details: ${catalogs_json}"
fi
if ! printf '%s' "$catalogs_json" | jq -e . >/dev/null 2>&1; then
  die "Unexpected catalogs list response (not JSON). Are you authenticated? Output: ${catalogs_json}"
fi

CATALOGS=()
while IFS= read -r c; do
  [[ -z "$c" ]] && continue
  if is_protected_catalog "$c"; then
    log "SKIP catalog (protected): ${c}"
    continue
  fi
  if [[ -n "$CATALOG_FILTER" && "$c" != "$CATALOG_FILTER" ]]; then
    continue
  fi
  CATALOGS+=("$c")
done < <(jq_names "$catalogs_json" "name" "catalogs")

if [[ -n "$CATALOG_FILTER" && ${#CATALOGS[@]} -eq 0 ]]; then
  if is_protected_catalog "$CATALOG_FILTER"; then
    die "Catalog '${CATALOG_FILTER}' is protected and cannot be cleaned"
  fi
  die "Catalog '${CATALOG_FILTER}' not found (or not visible to this identity)"
fi

if [[ ${#CATALOGS[@]} -eq 0 ]]; then
  log "No catalogs to clean."
  exit 0
fi

if [[ "$EXECUTE" -eq 0 ]]; then
  log "Mode: DRY-RUN (pass --yes to delete)"
else
  log "Mode: EXECUTE — deleting tables/views/volumes"
fi
log "Catalogs in scope: ${CATALOGS[*]}"
log ""

planned_tables=0
planned_volumes=0
deleted_tables=0
deleted_volumes=0
failed=0

delete_table() {
  local full_name="$1"
  local table_type="$2"
  if [[ "$EXECUTE" -eq 0 ]]; then
    log "  [dry-run] DROP ${table_type}: ${full_name}"
    return 0
  fi
  if dbx tables delete "$full_name" >/dev/null 2>&1; then
    log "  DELETED ${table_type}: ${full_name}"
    deleted_tables=$((deleted_tables + 1))
  else
    err "  FAILED ${table_type}: ${full_name}"
    failed=$((failed + 1))
  fi
}

delete_volume() {
  local full_name="$1"
  if [[ "$EXECUTE" -eq 0 ]]; then
    log "  [dry-run] DROP VOLUME: ${full_name}"
    return 0
  fi
  if dbx volumes delete "$full_name" >/dev/null 2>&1; then
    log "  DELETED VOLUME: ${full_name}"
    deleted_volumes=$((deleted_volumes + 1))
  else
    err "  FAILED VOLUME: ${full_name}"
    failed=$((failed + 1))
  fi
}

for catalog in "${CATALOGS[@]}"; do
  log "== Catalog: ${catalog} =="

  if ! schemas_json="$(dbx schemas list "$catalog" -o json 2>&1)"; then
    err "Failed to list schemas in ${catalog}: ${schemas_json}"
    failed=$((failed + 1))
    continue
  fi
  if ! printf '%s' "$schemas_json" | jq -e . >/dev/null 2>&1; then
    err "Unexpected schemas response for ${catalog}: ${schemas_json}"
    failed=$((failed + 1))
    continue
  fi

  while IFS= read -r schema; do
    [[ -z "$schema" ]] && continue
    if is_protected_schema "$schema"; then
      log "  SKIP schema (protected): ${catalog}.${schema}"
      continue
    fi

    log "  -- Schema: ${catalog}.${schema}"

    if ! tables_json="$(dbx tables list "$catalog" "$schema" -o json --omit-columns --omit-properties 2>&1)"; then
      err "  Failed to list tables in ${catalog}.${schema}: ${tables_json}"
      failed=$((failed + 1))
    elif printf '%s' "$tables_json" | jq -e . >/dev/null 2>&1; then
      while IFS=$'\t' read -r full_name table_type; do
        [[ -z "${full_name:-}" ]] && continue
        planned_tables=$((planned_tables + 1))
        delete_table "$full_name" "$table_type"
      done < <(jq_tables "$tables_json")
    else
      err "  Unexpected tables response for ${catalog}.${schema}: ${tables_json}"
      failed=$((failed + 1))
    fi

    if ! volumes_json="$(dbx volumes list "$catalog" "$schema" -o json 2>&1)"; then
      err "  Failed to list volumes in ${catalog}.${schema}: ${volumes_json}"
      failed=$((failed + 1))
    elif printf '%s' "$volumes_json" | jq -e . >/dev/null 2>&1; then
      while IFS= read -r vol_name; do
        [[ -z "${vol_name:-}" ]] && continue
        planned_volumes=$((planned_volumes + 1))
        delete_volume "$vol_name"
      done < <(jq_volume_names "$volumes_json")
    else
      err "  Unexpected volumes response for ${catalog}.${schema}: ${volumes_json}"
      failed=$((failed + 1))
    fi
  done < <(jq_names "$schemas_json" "name" "schemas")
  log ""
done

log "Summary"
log "  Tables/views planned: ${planned_tables}"
log "  Volumes planned:      ${planned_volumes}"
if [[ "$EXECUTE" -eq 1 ]]; then
  log "  Tables/views deleted: ${deleted_tables}"
  log "  Volumes deleted:      ${deleted_volumes}"
  log "  Failures:             ${failed}"
else
  log "  (dry-run - nothing deleted; re-run with --yes to apply)"
fi

if [[ "$failed" -gt 0 ]]; then
  exit 1
fi
