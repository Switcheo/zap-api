# ZAP-API

API server for Zilswap / $ZAP (Zilswap) Governance Token

## Building

You will need Rust (stable) installed.

```rust
cargo build
```

## Creating / Migrating Database

You will need Postgresql installed.

```bash
 diesel migration run
```

## Running

Configure your env vars by setting them in the .env file at the root of the binary, as part of the shell profile, or in the run command:

```env
BIND=127.0.0.1:3000
DATABASE_URL=postgres://localhost:5432/zap-api
VIEWBLOCK_API_KEY=xxx
VIEWBLOCK_API_SECRET=yyy
RUN_WORKER=true|false
NETWORK=mainnet|testnet
```

Run the server with:

```rust
cargo run
```

## Deployment

1. Build new binary for Linux:

    `cargo build --release` on a Linux machine (or host node, src can be found in ~/src/zap-api)

    The built binary can be found in `./target/release/zap-api`. Transfer this to `/opt/zap-api-<testnet|mainnet>`

2. Stop old node process:

    `sudo systemctl stop zap-api-<testnet|mainnet>`

3. Run migrations:

    `diesel migration run`

4. Run new node by restarting systemd (make sure env vars are correct in run command / .env file):

    `sudo systemctl start zap-api-<testnet|mainnet>`
