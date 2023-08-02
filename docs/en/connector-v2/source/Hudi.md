# Hudi

> Hudi source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Used to read data from Hudi. Currently, only supports hudi cow table and Snapshot Query with Batch Mode.

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

## Supported DataSource Info

:::tip

* Currently, only supports Hudi cow table and Snapshot Query with Batch Mode

:::

## Data Type Mapping

| Hudi Data type | Seatunnel Data type |
|----------------|---------------------|
| ALL TYPE       | STRING              |

## Source Options

|          Name           |  Type  |           Required           | Default |                                                                                              Description                                                                                              |
|-------------------------|--------|------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table.path              | String | Yes                          | -       | The hdfs root path of hudi table,such as 'hdfs://nameserivce/data/hudi/hudi_table/'.                                                                                                                  |
| table.type              | String | Yes                          | -       | The type of hudi table. Now we only support 'cow', 'mor' is not support yet.                                                                                                                          |
| conf.files              | String | Yes                          | -       | The environment conf file path list(local path), which used to init hdfs client to read hudi table file. The example is '/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml'. |
| use.kerberos            | bool   | No                           | false   | Whether to enable Kerberos, default is false.                                                                                                                                                         |
| kerberos.principal      | String | yes when use.kerberos = true | -       | When use kerberos, we should set kerberos principal such as 'test_user@xxx'.                                                                                                                          |
| kerberos.principal.file | string | yes when use.kerberos = true | -       | When use kerberos,  we should set kerberos principal file such as '/home/test/test_user.keytab'.                                                                                                      |
| common-options          | config | No                           | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.                                                                                              |

## Task Example

### Simple:

> This example reads from a Hudi COW table and configures Kerberos for the environment, printing to the console.

```hocon
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 2
  job.mode = "BATCH"
}
source{
  Hudi {
    table.path = "hdfs://nameserivce/data/hudi/hudi_table/"
    table.type = "cow"
    conf.files = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    use.kerberos = true
    kerberos.principal = "test_user@xxx"
    kerberos.principal.file = "/home/test/test_user.keytab"
  }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql/
}

sink {
    Console {}
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hudi Source Connector

