import ChangeLog from '../changelog/connector-clickhouse.md';

# ClickhouseFile

> Clickhouse file sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Generate the clickhouse data file with the clickhouse-local program, and then send it to the clickhouse
server, also call bulk load. This connector only support clickhouse table which engine is 'Distributed'.And `internal_replication` option
should be `true`. Supports Batch and Streaming mode.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

:::tip

Write data to Clickhouse can also be done using JDBC

:::

## Options

|          Name          |  Type   | Required |                Default                 | Description |
|------------------------|---------|----------|----------------------------------------|-------------|
| host                   | string  | yes      | -                                      | `ClickHouse` cluster address, the format is `host:port`, allowing multiple `hosts` to be specified, e.g. `"host1:8123,host2:8123"`. |
| database               | string  | yes      | -                                      | The `ClickHouse` database name. |
| table                  | string  | yes      | -                                      | The target table name. The table must use the `Distributed` engine with `internal_replication=true`. |
| username               | string  | yes      | -                                      | `ClickHouse` user username. |
| password               | string  | yes      | -                                      | `ClickHouse` user password. |
| clickhouse_local_path  | string  | yes      | -                                      | Absolute path of the `clickhouse-local` binary on every worker node (Spark/Flink TaskManager or Zeta worker). Each worker that runs a writer must have the same path; install the same binary at the same location on every node before starting the job. |
| sharding_key           | string  | no       | -                                      | Field used by the sharding algorithm when picking the shard node. When omitted, the writer picks a shard randomly. |
| copy_method            | string  | no       | scp                                    | Method used to transfer the staged file from the worker to the ClickHouse node. Supported values: `scp`, `rsync`. |
| node_free_password     | boolean | no       | false                                  | Set to `true` when every worker can SSH to every ClickHouse shard node without a password (key-based auth or ssh-agent). When `false`, configure `node_pass` so the writer knows how to authenticate. |
| node_pass              | list    | no       | -                                      | Per-node credentials for `scp`/`rsync`. Required when `node_free_password=false` and SSH key auth is not in place. |
| node_pass.node_address | string  | no       | -                                      | The address of the ClickHouse shard node that receives the file. |
| node_pass.username     | string  | no       | "root"                                 | The SSH username on the ClickHouse shard node. |
| node_pass.password     | string  | no       | -                                      | The SSH password on the ClickHouse shard node. Ignored when `key_path` is set and key-based auth succeeds. |
| compatible_mode        | boolean | no       | false                                  | Set to `true` for older ClickHouse releases where `clickhouse-local` does not understand the `--path` flag. The connector falls back to a path-free invocation that still produces a compatible staging file. |
| file_fields_delimiter  | string  | no       | "\t"                                   | Field delimiter used inside the staged CSV file. The value must be exactly one character; pick a character that never appears in any column value. |
| file_temp_path         | string  | no       | "/tmp/seatunnel/clickhouse-local/file" | Local directory on each worker where the staged file is written before being copied to ClickHouse. Use a path with enough free disk space for the largest batch the writer produces. |
| key_path               | string  | no       | -                                      | Absolute path of the SSH private key file used by `scp`/`rsync` when `node_free_password=false`. When set, `node_pass.password` is ignored; the key must be authorized on every shard node. |
| common-options         |         | no       | -                                      | Sink plugin common parameters, see [Sink Common Options](../common-options/sink-common-options.md). |

### host [string]

`ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .

### database [string]

The `ClickHouse` database

### table [string]

The table name

### username [string]

`ClickHouse` user username

### password [string]

`ClickHouse` user password

### sharding_key [string]

When ClickhouseFile split data, which node to send data to is a problem, the default is random selection, but the
'sharding_key' parameter can be used to specify the field for the sharding algorithm.

### clickhouse_local_path [string]

The address of the `clickhouse-local` program on every worker node (Spark executor, Flink TaskManager, or SeaTunnel Zeta worker).
Because each task needs to call `clickhouse-local`, every worker that runs a writer must have the binary at the same
absolute path before the job starts. A common pitfall is deploying the binary only on the driver/master node — the writer
runs on the workers, and a missing binary surfaces as `clickhouse-local: command not found` on the first batch.

### copy_method [string]

Specifies the method used to transfer files, the default is scp, optional scp and rsync

### node_free_password [boolean]

Because seatunnel need to use scp or rsync for file transfer, seatunnel need clickhouse server-side access.
If each spark node and clickhouse server are configured with password-free login,
you can configure this option to true, otherwise you need to configure the corresponding node password in the node_pass configuration

### node_pass [list]

Used to save the addresses and corresponding passwords of all clickhouse servers

### node_pass.node_address [string]

The address corresponding to the clickhouse server

### node_pass.username [string]

The username corresponding to the clickhouse server, default root user.

### node_pass.password [string]

The password corresponding to the clickhouse server.

### compatible_mode [boolean]

In the lower version of Clickhouse, the ClickhouseLocal program does not support the `--path` parameter,
you need to use this mode to take other ways to realize the `--path` parameter function

### file_fields_delimiter [string]

ClickhouseFile uses csv format to temporarily save data. If the data in the row contains the delimiter value
of csv, it may cause program exceptions.
Avoid this with this configuration. Value string has to be an exactly one character long

### file_temp_path [string]

The directory where ClickhouseFile stores temporary files locally.

### key_path [string]

The path of the private key file used for scp or rsync to connect to the ClickHouse server.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## How it works

ClickhouseFile is a **bulk-load** sink. Each writer:

1. Buffers incoming rows into a CSV file under `file_temp_path` on the local worker.
2. Once the buffer hits the configured size or the checkpoint barrier arrives, the worker shells out to
   `clickhouse-local` (located at `clickhouse_local_path`) to convert the CSV into ClickHouse's native
   storage format.
3. The worker copies the converted file to the target shard node via `scp` or `rsync`, using either
   password-less SSH, `node_pass` credentials, or a key file at `key_path`.
4. The shard node ingests the file through the `Distributed` table.

Because the final ingest happens on the ClickHouse side via `clickhouse-local`, the connector does not
participate in a distributed transaction and therefore cannot offer exactly-once. For end-to-end exactly-once
on ClickHouse, use the JDBC sink with the ReplacingMergeTree engine and a primary-key dedupe strategy
in the downstream table.

## Examples

### Minimal BATCH job

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  ClickhouseFile {
    host = "192.168.0.1:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    clickhouse_local_path = "/opt/clickhouse/usr/bin/clickhouse-local"
    sharding_key = "age"
    node_free_password = false
    node_pass = [{
      node_address = "192.168.0.1"
      password = "seatunnel"
    }]
  }
}
```

### Multi-shard cluster with password-less SSH

When the cluster has multiple shards and SSH trust is already configured cluster-wide, set
`node_free_password = true` and skip `node_pass` entirely. The connector will pick a shard based on
`sharding_key` and rely on the SSH config for authentication.

```hocon
sink {
  ClickhouseFile {
    host = "shard-1:8123,shard-2:8123,shard-3:8123"
    database = "default"
    table = "orders_dist"
    username = "default"
    password = ""
    clickhouse_local_path = "/usr/local/bin/clickhouse-local"
    sharding_key = "id"
    node_free_password = true
    copy_method = "rsync"
    file_temp_path = "/data/seatunnel/clickhouse-tmp"
  }
}
```

### SSH key-based authentication

When `node_free_password=false` but you want to authenticate with an SSH key instead of a plaintext
password, point `key_path` to the private key file. The key must be authorized on every shard node
(typically via `~/.ssh/authorized_keys`). `node_pass.password` is ignored when the key auth succeeds.

```hocon
sink {
  ClickhouseFile {
    host = "shard-1:8123,shard-2:8123"
    database = "default"
    table = "events_dist"
    username = "default"
    password = ""
    clickhouse_local_path = "/usr/local/bin/clickhouse-local"
    sharding_key = "user_id"
    node_free_password = false
    node_pass = [{
      node_address = "shard-1"
      username = "clickhouse"
    }, {
      node_address = "shard-2"
      username = "clickhouse"
    }]
    key_path = "/etc/seatunnel/id_rsa"
    copy_method = "rsync"
  }
}
```

### STREAMING job with checkpoint

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Kafka {
    # ...
  }
}

sink {
  ClickhouseFile {
    host = "shard-1:8123"
    database = "default"
    table = "events_dist"
    username = "default"
    password = ""
    clickhouse_local_path = "/usr/local/bin/clickhouse-local"
    node_free_password = true
    copy_method = "rsync"
  }
}
```

## Changelog

<ChangeLog />

