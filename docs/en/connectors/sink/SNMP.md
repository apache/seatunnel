import ChangeLog from '../changelog/connector-snmp.md';

# SNMP

> SNMPv2c SET sink connector

## Description

The SNMP sink writes each input row to one SNMP agent by sending one synchronous SNMPv2c SET request.
The V1 scope is deliberately limited to SET operations. It does not send traps or informs, and it does not support SNMPv1 or SNMPv3.

Every row supplies a numeric OID, a string value, and an SMI value type. The corresponding field names are configurable.
The default mapping consumes the `oid`, `value`, and `value_type` fields emitted by the SNMP source; additional fields such as
`agent` and `poll_time` are ignored by the sink.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

The connector uses SNMP4J and supports SNMPv2c agents reachable over UDP.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| SNMP agent | SNMPv2c            | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-snmp) |

## Sink Options

| Name             | Type   | Required | Default      | Description |
|------------------|--------|----------|--------------|-------------|
| host             | String | Yes      | -            | SNMP agent host name or IP address. Do not include a protocol or port. |
| port             | Int    | No       | 161          | SNMP agent UDP port. |
| community        | String | Yes      | -            | SNMPv2c community credential. The connector does not write this value to its logs or errors. |
| timeout_millis   | Long   | No       | 5000         | Timeout in milliseconds for each SET request attempt. |
| retries          | Int    | No       | 1            | Number of retries after the initial SET request attempt. A value of `0` sends one attempt. |
| oid_field        | String | No       | oid          | Input `STRING` field containing the numeric OID to set. |
| value_field      | String | No       | value        | Input `STRING` field containing the value to set. |
| value_type_field | String | No       | value_type   | Input `STRING` field containing the SMI value type. |

The three mapped fields must exist in the input schema, must use `STRING`, and must refer to distinct fields. Schema errors are
rejected while the job is created. Null values and blank OID or value-type fields are rejected before a network request is sent. The value field is validated according to its SMI type; an empty `OctetString` or `OctetStringHex` is valid, and text `OctetString` whitespace is preserved.

## Supported SMI Value Types

The `value_type` comparison is case-insensitive and ignores whitespace, `_`, and `-` characters.
The sink accepts both the documented names and SNMP4J syntax strings emitted by the SNMP source,
including `Counter`, `Gauge`, `OCTET STRING`, and `OBJECT IDENTIFIER`.

| Value type | Accepted value |
|------------|----------------|
| `Integer32` or `Integer` | Signed 32-bit decimal integer. |
| `UnsignedInteger32` or `UnsignedInteger` | Decimal integer from 0 through 4294967295. |
| `Counter32` or `Counter` | Decimal integer from 0 through 4294967295. |
| `Gauge32` or `Gauge` | Decimal integer from 0 through 4294967295. |
| `TimeTicks` | Decimal count of hundredths of a second from 0 through 4294967295, or the SNMP4J source format `[days, ]hours:mm:ss.hh`. |
| `Counter64` | Decimal integer from 0 through 18446744073709551615. |
| `OctetString` or `OCTET STRING` | UTF-8 text represented by the input string. |
| `OctetStringHex` | An even number of hexadecimal characters, such as `00ff10`. |
| `OID` or `OBJECT IDENTIFIER` | Numeric object identifier. Leading dots are accepted. |
| `IpAddress` | Dotted IPv4 address. |

`OctetString` is a textual mapping. Use `OctetStringHex` when byte-for-byte binary content is required.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  shade.options = ["community"]
}

source {
  FakeSource {
    plugin_output = "snmp_updates"
    schema = {
      fields {
        oid = string
        value = string
        value_type = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = {
          oid = "1.3.6.1.2.1.1.5.0"
          value = "router-1"
          value_type = "OctetString"
        }
      }
    ]
  }
}

sink {
  SNMP {
    plugin_input = "snmp_updates"
    host = "192.0.2.10"
    port = 161
    community = ${SNMP_COMMUNITY}
    timeout_millis = 3000
    retries = 1
  }
}
```

`${SNMP_COMMUNITY}` is resolved through the normal SeaTunnel configuration substitution path.
Set the value outside the checked-in job file. Adding `community` to `shade.options` also keeps it
masked if the parsed job configuration is logged.

## Delivery, Failure, and Security Behavior

- One successful `write` call means the agent returned a successful SNMP response for that row.
- A timeout after all configured attempts or a non-zero SNMP response error status fails the sink task.
- A row can block for approximately `timeout_millis * (retries + 1)` before it fails. Keep this below the job's checkpoint timeout.
- SNMP4J retransmits a timed-out request. A late response can therefore make a non-idempotent OID observe the same SET more than once.
- The sink has no transactional commit protocol or recoverable writer state. Engine recovery can repeat a SET request, so delivery is at-least-once.
- Parallel writers can update the same OID out of order. Use parallelism 1 when update order matters.
- Row kinds are not interpreted as CDC operations. Every input row, including update or delete row kinds, is treated as a SET request.
- Treat `community` as a credential. Supply it through configuration substitution or another secret-management path, and do not place a real value in job files committed to source control.
- SNMPv2c provides no wire encryption or integrity protection. The community and SET payload are sent in cleartext; use only a trusted private network or a protected tunnel such as a VPN.
- Traps, informs, SNMPv1, and SNMPv3 are outside this V1 contract.

See [Common Sink Options](../common-options/sink-common-options.md) for options such as `plugin_input`.

<ChangeLog />
