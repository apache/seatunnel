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
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

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
| node_pass | list | yes | - | Per-node SSH credentials. Supply an empty list for passwordless SSH with the default `root` user. Each configured entry needs `node_address` and `password`. |
| node_pass.node_address | string | yes for each record | - | Hostname of the ClickHouse shard node that receives the file. |
| node_pass.username     | string  | no       | "root"                                 | The SSH username on the ClickHouse shard node. |
| node_pass.password | string | yes for each record | - | Per-node SSH password. Use `""` in the SCP key authentication example. |
| compatible_mode        | boolean | no       | false                                  | Set to `true` for older ClickHouse releases where `clickhouse-local` does not understand the `--path` flag. The connector falls back to a path-free invocation that still produces a compatible staging file. |
| file_fields_delimiter  | string  | no       | "\t"                                   | Field delimiter used inside the staged CSV file. The value must be exactly one character; pick a character that never appears in any column value. |
| file_temp_path         | string  | no       | "/tmp/seatunnel/clickhouse-local/file" | Local directory on each worker where the staged file is written before being copied to ClickHouse. Use a path with enough free disk space for the largest batch the writer produces. |
| key_path | string | no | - | Absolute path to the SSH private key on the worker. See the SCP key authentication example for per-node entries. |
| server_time_zone | string | no | ZoneId.systemDefault() | Time zone used by the ClickHouse connection. |
| clickhouse.config | map | no | - | Additional ClickHouse Java client options. |
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

Note: `node_pass` is a list; every entry must contain `node_address` and `password` (`username` falls back to `root` when not configured). Missing either one fails job initialization.

### node_pass.node_address [string]

The address corresponding to the clickhouse server. Required inside each `node_pass` entry.

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

Path to the SSH private key on the worker. With `rsync`, the file transfer uses password authentication whenever the password is non-null, including an empty string.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## How it works

ClickhouseFile is a **bulk-load** sink. Each writer:

1. Buffers incoming rows into a CSV file under `file_temp_path` on the local worker.
2. During `prepareCommit()`, when a checkpoint or the end of a batch job triggers commit preparation, the worker invokes
   `clickhouse-local` (located at `clickhouse_local_path`) to convert the CSV into ClickHouse's native
   storage format.
3. The worker copies the converted file to the target shard node via `scp` or `rsync`, using either
   password-less SSH, `node_pass` credentials, or a key file at `key_path`.
4. The aggregate committer executes `ALTER TABLE ... ATTACH PART` on the shard's local table.

Because the final ingest happens on the ClickHouse side via `clickhouse-local`, the connector does not
participate in a distributed transaction and therefore cannot offer exactly-once. Choose a sink and target-table design whose documented delivery guarantees match your recovery requirements.

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
`node_free_password = true` and `node_pass = []`. The current factory always parses the list. The connector will pick a shard based on
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
    node_pass = []
    copy_method = "rsync"
    file_temp_path = "/data/seatunnel/clickhouse-tmp"
  }
}
```

### SSH key-based authentication

Use `copy_method = "scp"` to transfer files over the SSH session authenticated with the private
key at `key_path`. Each `node_pass` entry still needs a `password` field during configuration
parsing, so the example sets it to `""`. Add the public key to `~/.ssh/authorized_keys` on each shard.

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
      password = ""
    }, {
      node_address = "shard-2"
      username = "clickhouse"
      password = ""
    }]
    key_path = "/etc/seatunnel/id_rsa"
    copy_method = "scp"
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
    node_pass = []
    copy_method = "rsync"
  }
}
```

## Changelog

<ChangeLog />

