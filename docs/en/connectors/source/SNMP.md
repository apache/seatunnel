import ChangeLog from '../changelog/connector-snmp.md';

# SNMP

> SNMPv2c polling source connector

## Description

The SNMP source polls an SNMP agent over UDP by sending an SNMPv2c GET request for an explicit list of numeric OIDs.
Each returned variable binding becomes one row. This first version does not perform WALK, GETNEXT, or GETBULK.

Batch jobs perform exactly one poll and then finish. Streaming jobs repeat the same GET request after
`poll_interval_millis`. The source uses one split, so its parallelism must be 1.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

The connector uses SNMP4J and supports SNMPv2c agents reachable over UDP.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| SNMP agent | SNMPv2c            | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-snmp) |

## Source Options

| Name                 | Type         | Required | Default | Description |
|----------------------|--------------|----------|---------|-------------|
| host                 | String       | Yes      | -       | SNMP agent host name or IP address. Do not include a protocol or port. |
| port                 | Int          | No       | 161     | SNMP agent UDP port. |
| community            | String       | Yes      | -       | SNMPv2c community credential. The connector does not write this value to its logs or errors. |
| oids                 | List\<String\> | Yes   | -       | Explicit numeric OIDs sent in the GET request. Leading dots are accepted. Duplicate and symbolic OIDs are rejected. |
| timeout_millis       | Long         | No       | 5000    | Timeout in milliseconds for each request attempt. |
| retries              | Int          | No       | 1       | Number of retries after the initial request attempt. A value of `0` sends one attempt. |
| poll_interval_millis | Long         | No       | 60000   | Delay in milliseconds between completed streaming polls. Ignored by batch jobs. |

## Output Schema

The output schema is fixed and must not be configured with a `schema` option.

| Field      | Type   | Description |
|------------|--------|-------------|
| agent      | string | Polled agent in `host:port` form. |
| oid        | string | Numeric OID returned by the agent. |
| value      | string | SNMP4J string representation of the returned value. |
| value_type | string | SMI type name, such as `Integer32`, `OctetString`, `TimeTicks`, or `noSuchInstance`. |
| poll_time  | long   | Epoch time in milliseconds captured when the poll starts. All rows from one poll share this value. |

SNMP values use different SMI types. V1 preserves the type name and exposes SNMP4J's textual representation
instead of coercing all values into one numeric type. Binary values may not round-trip byte-for-byte through the
`value` field.

## Batch Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  SNMP {
    plugin_output = "snmp_metrics"
    host = "192.0.2.10"
    port = 161
    community = ${SNMP_COMMUNITY}
    oids = [
      "1.3.6.1.2.1.1.3.0",
      "1.3.6.1.2.1.1.5.0"
    ]
    timeout_millis = 3000
    retries = 1
  }
}

sink {
  Console {}
}
```

## Streaming Example

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  SNMP {
    plugin_output = "snmp_metrics"
    host = "192.0.2.10"
    community = ${SNMP_COMMUNITY}
    oids = ["1.3.6.1.2.1.1.3.0"]
    timeout_millis = 3000
    retries = 2
    poll_interval_millis = 30000
  }
}

sink {
  Console {}
}
```

## Failure and Security Behavior

- A timeout after all configured attempts fails the source task.
- A non-zero SNMP response error status fails the source task; partial response rows are not emitted.
- Closing or cancelling the source closes the SNMP transport and stops later polls.
- Treat `community` as a credential. Supply it through configuration substitution or another secret-management path,
  and do not place a real value in job files committed to source control.

<ChangeLog />
