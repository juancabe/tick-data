# TickData

App to collect data from [HyperLiquid's public WebSocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)'s public `WebSocket`, store it and safely persist it compressed in `parquet` files.

Collected data can be personalized, you can opt out of every piece of data collected. 

## Configuration

- *RUST_LOG*: Configures `log` level, `info` is recommended.
- *HOT_BUDGET*: \~Defines how much _uncompressed disk space_ and _RAM_ will each _hot-file-related_ data (each coin `trade` and `mids` use one hot file) consume.
  RAM can spike when some transform operations are done, so expect 2x average RAM usage during some moments.
    An example of resource usage can be:
    HOT_BUDGET=125000000, `3 coins` + `mids` configured ->
      125 MB * 4 = \~0.5 GB of average usage, can spike to \~1 GB
- *MIDS_ENABLED*: Possible values \[true | false\] can enable / disable collecting `all_mids`.
- *TRADE_COINS*: List of symbols that will define which subscription to the `trades` stream'll be made. 

## How to run SystemD Service

Here we'll detail how to set up the _systemd service_ for *Tick Data* app.

### .service File

The main file ([/etc/systemd/system/tick-data.service](./systemd/tick-data.service)) to run the service

#### Description

It sets up a SystemD dynamic (sandboxed) user, create this `.service` file at `/etc/systemd/system` dir and be sure the first time the service is launched `StateDirectory` doesn't exist on `/var/lib`.

#### Requirements

It needs the configuration file [`/etc/default/tick-data.env`](./systemd/tick-data.env) to exist *(and be readable by `root`)*.
