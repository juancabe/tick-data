# TickData

App to collect data from [HyperLiquid]()'s public `WebSocket`, store it and safely persist it compressed in `parquet` files.

Collected data can be personalized, you can opt out of every piece of data collected. 

## Configuration

TODO

## How to run SystemD Service

Here we'll detail how to set up the _systemd service_ for *Tick Data* app.

## .service File

The main file ([/etc/systemd/system/tick-data.service](./systemd/tick-data.service)) to run the service

### Description

It sets up a SystemD dynamic (sandboxed) user, create this `.service` file at `/etc/systemd/system` dir and be sure the first time the service is launched `StateDirectory` doesn't exist on `/var/lib`.

### Requirements

It needs the configuration file [`/etc/default/tick-data.env`](./systemd/tick-data.env) to exist *(and be readable by `root`)*.
