#!/bin/sh
# Dependency-free fake Singer tap for faucet-source-singer tests.
#
# Emits a scripted RECORD/STATE NDJSON sequence on stdout. Behavior is driven by
# flags scanned out of the full argv (faucet always prepends --config/--catalog/
# --state; extra args come from the config's `args`):
#
#   --stream NAME          stream name for RECORD/SCHEMA (default: s)
#   --total N              highest record id to emit (default: 5)
#   --state-at "a,b,c"     emit a STATE {"last_id": id} after these ids
#   --crash-after-new N    after emitting N *new* records THIS run, exit 1
#   --state FILE           (faucet-provided) resume file; we read "last_id"
#
# Resume semantics (deliberately COARSE, like many real taps): on resume from a
# persisted state we RE-EMIT the boundary record (id == last_id) as well, so the
# downstream idempotent/upsert sink must dedup a 1-record overlap. This is what
# makes the crash-resume no-dup test meaningful.

stream=s
total=5
state_at=""
crash_after_new=""
state_file=""

# Scan every argument; ignore --config/--catalog and their values.
while [ $# -gt 0 ]; do
  case "$1" in
    --stream) stream="$2"; shift 2 ;;
    --total) total="$2"; shift 2 ;;
    --state-at) state_at="$2"; shift 2 ;;
    --crash-after-new) crash_after_new="$2"; shift 2 ;;
    --state) state_file="$2"; shift 2 ;;
    --config|--catalog) shift 2 ;;
    *) shift 1 ;;
  esac
done

# Read resume cursor (last_id) from the state file if present.
last_id=0
if [ -n "$state_file" ] && [ -f "$state_file" ]; then
  parsed=$(sed -n 's/.*"last_id"[ ]*:[ ]*\([0-9][0-9]*\).*/\1/p' "$state_file" | head -n1)
  if [ -n "$parsed" ]; then
    last_id="$parsed"
  fi
fi

# Emit a SCHEMA first (pass-through on faucet's side).
printf '{"type":"SCHEMA","stream":"%s","schema":{"type":"object","properties":{"id":{"type":"integer"}}},"key_properties":["id"]}\n' "$stream"

# Coarse resume: start at last_id (re-emit the boundary record) when resuming,
# else at 1.
if [ "$last_id" -gt 0 ]; then
  start=$last_id
else
  start=1
fi

emitted_new=0
id=$start
while [ "$id" -le "$total" ]; do
  printf '{"type":"RECORD","stream":"%s","record":{"id":%s,"name":"row-%s"}}\n' "$stream" "$id" "$id"
  emitted_new=$((emitted_new + 1))

  if [ -n "$crash_after_new" ] && [ "$emitted_new" -ge "$crash_after_new" ]; then
    # Crash before emitting the STATE for this id — nothing past the last
    # committed STATE is checkpointed.
    echo "fake_tap: simulated crash after $emitted_new new records" >&2
    exit 1
  fi

  case ",$state_at," in
    *",$id,"*)
      printf '{"type":"STATE","value":{"last_id":%s}}\n' "$id"
      ;;
  esac
  id=$((id + 1))
done

# Final STATE covering everything (clean completion).
printf '{"type":"STATE","value":{"last_id":%s}}\n' "$total"
