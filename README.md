# TickData

App to collect data from [*HyperLiquid*'s public WebSocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket), store it and safely persist it compressed in `parquet` files.

Collected data can be personalized, you can opt out of every piece of data collected. 

## Configuration

- *RUST_LOG*: Configures `log` level, `info` is recommended.
- *WORK_DIR*: Base directory for data storage. The app creates:
  - `trades/<COIN>/hot-storage` and `trades/<COIN>/compressed-storage`
  - `mids/<DEX>_DEX/hot-storage` and `mids/<DEX>_DEX/compressed-storage`
- *HOT_BUDGET*: Defines the max hot storage size per pipeline (each coin trades and each dex mids).
  RAM can spike during transforms, so expect up to ~2x average RAM usage.
  Example: HOT_BUDGET=125000000 with 3 coins + 2 dex mids -> 5 pipelines
  125 MB * 5 = ~0.625 GB average, can spike to ~1.25 GB
- *TRADE_COINS*: Comma-separated list of symbols to subscribe to the trades stream.
- *DEFAULT_DEX_MIDS_ENABLED*: Possible values [true | false] to enable mids for the default dex.
- *MIDS_NON_DEFAULT_DEXES*: Comma-separated list of dex identifiers to subscribe to `all_mids`.

## How to run SystemD Service

Here we'll detail how to set up the _systemd service_ for *Tick Data* app.

### .service File

The main file ([/etc/systemd/system/tick-data.service](./systemd/tick-data.service)) to run the service

#### Description

It sets up a SystemD dynamic (sandboxed) user, create this `.service` file at `/etc/systemd/system` dir and be sure the first time the service is launched `StateDirectory` doesn't exist on `/var/lib`.

#### Requirements

It needs the configuration file [`/etc/default/tick-data.env`](./systemd/tick-data.env) to exist *(and be readable by `root`)*.

## TODOs

- [ ] Improve in-code documentation
- [ ] Add new interesting subscriptions

