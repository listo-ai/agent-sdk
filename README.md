# agent-sdk

Public Rust SDK for block authors. Depend on these crates to build blocks
without cloning the agent source.

## Crates

| Crate | Published as | Purpose |
|-------|--------------|---------|
| `blocks-sdk` | `listo-blocks-sdk` | `NodeBehavior`, `NodeKind` derive, `WasmPlugin`, process-block runner |
| `blocks-sdk-macros` | `listo-blocks-sdk-macros` | Proc macros for the SDK |
| `block-client` | `listo-block-client` | `BlockContext`, `ActionResult`, ComponentTree builder helpers, test harness |
| `block-domain` | `listo-block-domain` | Reusable domain patterns (`StateMachine`, `Prioritised`, `AssignmentSet`) |

## Build

```bash
cargo build --workspace
cargo test --workspace
```

## Dependencies

- [`contracts`](../contracts) — wire types (`listo-spi`)
- [`agent-client-rs`](../agent-client-rs) — HTTP client (for `block-client`)

## Writing a block

See [`blocks/`](../blocks) for reference implementations.

Part of the [listo-ai workspace](../).
