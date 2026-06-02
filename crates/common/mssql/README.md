# faucet-common-mssql

Shared configuration, TLS, and connection-pool types for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) Microsoft SQL Server
**source** ([`faucet-source-mssql`](https://crates.io/crates/faucet-source-mssql))
and **sink** ([`faucet-sink-mssql`](https://crates.io/crates/faucet-sink-mssql))
connectors. Built on [`tiberius`](https://crates.io/crates/tiberius) +
[`bb8-tiberius`](https://crates.io/crates/bb8-tiberius).

You normally don't depend on this crate directly — both connectors re-export the
types you configure. It exists so the source and sink share one connection /
TLS / pooling implementation (per the faucet "source/sink pair shares a
`faucet-common-<name>` crate" convention).

## What's here

| Item | Purpose |
|------|---------|
| `MssqlConnectionConfig` | `connection_url` **or** `connection_string` (exactly one) + a `tls` block. Flattened into both end configs. |
| `MssqlTls` / `MssqlTlsMode` | Encryption mode (`prefer` / `require` / `trust_server_certificate` / `disable`) + optional `ca_cert_path`. |
| `build_config` | `MssqlConnectionConfig` → `tiberius::Config` (URL parse, ADO passthrough, TLS). |
| `build_pool` | Builds a `bb8` pool and eagerly validates one connection (fail-fast). |
| `with_statement_timeout` | Wraps a query future in a timeout. |
| `quote_ident_mssql` | `[bracket]` identifier quoting (doubles interior `]`). |
| `PARAM_LIMIT` | `2100` — MSSQL's per-request bind-parameter ceiling. |

## Connection

```yaml
# URL form — faucet parses host/port/database/credentials; the tls block
# governs encryption.
connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
tls:
  type: prefer          # prefer | require | trust_server_certificate | disable
  ca_cert_path: null

# …or an ADO.NET-style string handed straight to tiberius:
connection_string: "Server=tcp:localhost,1433;Database=sales;User Id=sa;Password=...;"
```

Special characters in URL credentials must be percent-encoded
(`@` → `%40`, `:` → `%3A`, `/` → `%2F`).

### TLS modes

| Mode | `tiberius` encryption | Notes |
|------|-----------------------|-------|
| `prefer` (default) | `On` | Encrypt the connection (safe modern default). |
| `require` | `Required` | Fail if the server doesn't offer TLS. |
| `trust_server_certificate` | `On` + `trust_cert()` | Accepts self-signed certs. **Insecure against MITM — dev only.** |
| `disable` | `NotSupported` | No transport encryption. |

`ca_cert_path` trusts a specific CA certificate for server validation (ignored
when `disable`).

## Auth

SQL Server authentication (username + password) only. Windows / Integrated
authentication and Azure AD / Managed Identity are out of scope for v1.

## License

MIT OR Apache-2.0.
