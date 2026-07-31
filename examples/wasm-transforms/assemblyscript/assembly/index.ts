// Reference faucet-stream WASM transform, written in AssemblyScript.
//
// Implements the v1 ABI: uppercase a top-level string `name` and stamp
// `wasm_processed: true`. Build with `npm install && npm run build`, which
// emits `build/transform.wasm`.
//
// AssemblyScript exports its own `memory`, and we expose `alloc`/`free` backed
// by the runtime allocator. JSON is handled with assemblyscript-json.

import { JSON } from "assemblyscript-json/assembly";

// Last error message bytes, kept for error_ptr()/error_len().
let errPtr: i32 = 0;
let errLen: i32 = 0;

// Allocate `len` bytes the host can write the input into. `__new` returns a
// managed pointer; we pin it so the GC won't move/free it before `free`.
export function alloc(len: i32): i32 {
  const ptr = __new(len, idof<ArrayBuffer>());
  __pin(ptr);
  return ptr;
}

export function free(ptr: i32, _len: i32): void {
  __unpin(ptr);
}

export function error_ptr(): i32 {
  return errPtr;
}

export function error_len(): i32 {
  return errLen;
}

function setError(msg: string): u64 {
  const bytes = String.UTF8.encode(msg);
  const len = bytes.byteLength;
  const ptr = alloc(len);
  memory.copy(ptr, changetype<usize>(bytes), len);
  errPtr = ptr;
  errLen = len;
  return u64.MAX_VALUE;
}

function emit(json: string): u64 {
  const bytes = String.UTF8.encode(json);
  const len = bytes.byteLength;
  const ptr = alloc(len);
  memory.copy(ptr, changetype<usize>(bytes), len);
  return (u64(ptr) << 32) | u64(len);
}

export function transform(ptr: i32, len: i32): u64 {
  const input = String.UTF8.decodeUnsafe(ptr, len);
  const parsed = JSON.parse(input);
  if (!parsed.isObj) {
    return setError("expected a JSON object");
  }
  const obj = <JSON.Obj>parsed;

  const name = obj.getString("name");
  if (name != null) {
    obj.set("name", name.valueOf().toUpperCase());
  }
  obj.set("wasm_processed", true);

  return emit(obj.stringify());
}
