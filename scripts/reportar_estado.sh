#!/usr/bin/env bash
# Avisa al portal (actualizar-estado) del estado de las cargas ok de cargas_lote.json.
# Uso: reportar_estado.sh ESTADO [DETALLE_ERROR] [ARCHIVO]
# ESTADO=intake: las ok pasan a "pendientes" y cada rechazada/duplicada se avisa con su motivo.
set -euo pipefail
estado=${1:?estado}; detalle=${2:-}; f=${3:-${RUNNER_TEMP:-.}/cargas_lote.json}
[[ -n "${SUPABASE_FUNCTIONS_URL:-}" && -n "${PIPELINE_WEBHOOK_SECRET:-}" && -s "$f" ]] || exit 0
jq -c --arg e "$estado" --arg d "$detalle" --arg u "${RUN_URL:-}" '
  ({carga_ids: [.[] | select(.resultado == "ok") | .carga_id], estado: (if $e == "intake" then "pendientes" else $e end), github_run_url: $u}
    + (if $d == "" then {} else {detalle_error: $d} end) | select(.carga_ids | length > 0)),
  (if $e == "intake" then .[] | select(.resultado != "ok") | {carga_ids: [.carga_id], estado: .resultado, detalle_error: (.motivo // ""), github_run_url: $u} else empty end)' "$f" |
while read -r body; do
  curl --fail-with-body -sS -X POST "$SUPABASE_FUNCTIONS_URL/actualizar-estado" \
    -H "Authorization: Bearer $PIPELINE_WEBHOOK_SECRET" -H 'content-type: application/json' -d "$body" || echo "::warning::no se pudo avisar al portal"
done
