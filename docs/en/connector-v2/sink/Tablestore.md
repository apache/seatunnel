# Tablestore

> Tablestore sink connector

## Description

Write data to `Tablestore`

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| end_point         | string | yes      | -             |
| instance_name     | string | yes      | -             |
| access_key_id     | string | yes      | -             |
| access_key_secret | string | yes      | -             |
| table             | string | yes      | -             |
| primary_keys      | array  | yes      | -             |
| batch_size        | string | no       | 25            |
| common-options    | config | no       | -             |

### end_point [string]

endPoint to write to Tablestore.

### instanceName [string]

The instanceName of Tablestore.

### access_key_id [string]

The access id of Tablestore.

### access_key_secret [string]

The access secret of Tablestore.

### table [string]

The table of Tablestore.

### primaryKeys [array]

The primaryKeys of Tablestore.

### common options [ config ]

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

## Example

```bash
Tablestore {
    end_point = "xxxx"
    instance_name = "xxxx"
    access_key_id = "xxxx"
    access_key_secret = "xxxx"
    table = "sink"
    primary_keys = ["pk_1","pk_2","pk_3","pk_4"]
  }
```

## Changelog

### next version

- Add Tablestore Sink Connector

