# AssemblyScript reference WASM transform

Implements the faucet-stream v1 `wasm` transform ABI in AssemblyScript.

```bash
npm install
npm run build          # → build/transform.wasm
```

Then point a pipeline at it:

```yaml
transforms:
  - type: wasm
    config:
      module: examples/wasm-transforms/assemblyscript/build/transform.wasm
```

Notes:
- `--exportRuntime` exposes `__new`/`__pin`/`__unpin`, which back our
  `alloc`/`free` exports, and the standard `memory` export.
- `assemblyscript-json` handles parsing/serialization.
