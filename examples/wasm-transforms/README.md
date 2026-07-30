# Reference WASM transform modules

These are runnable reference modules for the faucet-stream [`wasm` transform](../../docs/book/src/cookbook/wasm-transforms.md).
Each implements the same v1 ABI in a different source language and does the same
thing: uppercase a top-level `name` field and stamp `wasm_processed: true`.

| Language | Source | Build |
|---|---|---|
| Rust | [`rust/`](./rust/) | `cargo build --release --target wasm32-unknown-unknown` |
| AssemblyScript | [`assemblyscript/`](./assemblyscript/) | `npm install && npm run build` |

Any toolchain that emits core wasm exporting the ABI below works (TinyGo,
Zig, C/C++, …); these two are the maintained references.

`add_field.wasm` (checked in at this directory) is the Rust build, used by the
runnable example `cli/examples/csv_to_jsonl_wasm.yaml` and by the CLI
integration test. Rebuild it with:

```bash
cd rust
cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/faucet_wasm_example_add_field.wasm ../add_field.wasm
```

## The ABI in one paragraph

The host writes the record as UTF-8 JSON into memory returned by your exported
`alloc(len) -> ptr`, then calls `transform(ptr, len) -> u64`. The return packs
`(out_ptr << 32) | out_len`; `0` drops the record, `u64::MAX` signals an error
(message via the optional `error_ptr()`/`error_len()` exports), and anything
else points at the output UTF-8 JSON. Export `memory`, and optionally
`free(ptr, len)` so the host can reclaim buffers. Full details:
[cookbook](../../docs/book/src/cookbook/wasm-transforms.md).

## Try it

```bash
# from the repo root
cargo run -p faucet-cli --features transform-wasm -- \
  run cli/examples/csv_to_jsonl_wasm.yaml
cat /tmp/faucet_wasm_demo.jsonl
```
