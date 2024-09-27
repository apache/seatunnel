# Http

> Http sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Description

Used to launch web hooks using data.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http sink only support `post json` webhook and the data from source will be treated as body content in web hook.**

## Supported DataSource Info

In order to use the Http connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                         |
|------------|--------------------|------------------------------------------------------------------------------------|
| Http       | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http) |

## Sink Options

|            Name             |  Type  | Required | Default |                                                 Description                                                 |
|-----------------------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| url                         | String | Yes      | -       | Http request url                                                                                            |
| headers                     | Map    | No       | -       | Http headers                                                                                                |
| retry                       | Int    | No       | -       | The max retry times if request http return to `IOException`                                                 |
| retry_backoff_multiplier_ms | Int    | No       | 100     | The retry-backoff times(millis) multiplier if request http failed                                           |
| retry_backoff_max_ms        | Int    | No       | 10000   | The maximum retry-backoff times(millis) if request http failed                                              |
| connect_timeout_ms          | Int    | No       | 12000   | Connection timeout setting, default 12s.                                                                    |
| socket_timeout_ms           | Int    | No       | 60000   | Socket timeout setting, default 60s.                                                                        |
| common-options              |        | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details |

## Example

simple:

```hocon
Http {
    url = "http://localhost/test/webhook"
    headers {
        token = "9e32e859ef044462a257e1fc76730066"
    }
}
```

### Multiple table

#### example1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Http {
    ...
    url = "http://localhost/test/${database_name}_test/${table_name}_test"
  }
}
```

#### example2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  Http {
    ...
    url = "http://localhost/test/${schema_name}_test/${table_name}_test"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Http Sink Connector

