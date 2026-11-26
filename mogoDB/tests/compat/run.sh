#!/usr/bin/env bash
# Differential compatibility test: run each tests/compat/*.js script against a real
# mongod (REF_PORT, default 27018) and mogod (TEST_PORT, default 27017) and diff output.
#
#   tests/compat/run.sh [script.js ...]
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
ref="${REF_PORT:-27018}"
test="${TEST_PORT:-27017}"
out="${OUT_DIR:-$here/out}"
mkdir -p "$out"
scripts=("$@")
[[ ${#scripts[@]} -eq 0 ]] && scripts=("$here"/t_*.js)
fail=0
for s in "${scripts[@]}"; do
  n="$(basename "$s" .js)"
  mongosh --quiet --port "$ref" --file "$here/lib.js" --file "$s" >"$out/$n.ref" 2>&1
  mongosh --quiet --port "$test" --file "$here/lib.js" --file "$s" >"$out/$n.test" 2>&1
  if diff -u "$out/$n.ref" "$out/$n.test" >"$out/$n.diff"; then
    echo "PASS $n ($(wc -l <"$out/$n.ref") cases)"
  else
    echo "FAIL $n: $(grep -c '^+[^+]' "$out/$n.diff") differing lines (see $out/$n.diff)"
    fail=1
  fi
done
exit $fail
