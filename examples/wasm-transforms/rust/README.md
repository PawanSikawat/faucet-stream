# Rust reference WASM transform

Implements the faucet-stream v1 `wasm` transform ABI. Per record it uppercases a
top-level string `name` and stamps `wasm_processed: true`.

## Build

```bash
rustup target add wasm32-unknown-unknown   # once
cargo build --release --target wasm32-unknown-unknown
# → target/wasm32-unknown-unknown/release/faucet_wasm_example_add_field.wasm
```

Copy it next to the other reference modules (this is the committed `add_field.wasm`):

```bash
cp target/wasm32-unknown-unknown/release/faucet_wasm_example_add_field.wasm ../add_field.wasm
```

This crate is intentionally **excluded from the host workspace** (it targets
`wasm32-unknown-unknown` with its own release profile), so a normal
`cargo build --workspace` never touches it.
