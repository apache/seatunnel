# Tablestore

> Tablestore source connector

## Description

Read data from Alicloud Tablestore，support full and CDC.


## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [X] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                  | type   | required | default value |
|-----------------------|--------|----------|---------------|
| end_point             | string | yes      | -             |
| instance_name         | string | yes      | -             |
| access_key_id         | string | yes      | -             |
| access_key_secret     | string | yes      | -             |
| table                 | string | yes      | -             |
| primary_keys          | array  | yes      | -             |
| schema                | config | yes      | -             |


### end_point [string]

The endpoint of Tablestore.

### instance_name [string]

The intance name of Tablestore.

### access_key_id [string]

The access id of Tablestore.

### access_key_secret [string]

The access secret of Tablestore.

### table [string]

The table name of Tablestore.

### primary_keys [array]

The primarky key of table,just add a unique primary key.

### schema [Config]



## Example

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Tablestore {
    end_point = "https://****.cn-zhangjiakou.tablestore.aliyuncs.com"
    instance_name = "****"
    access_key_id="***************2Ag5"
    access_key_secret="***********2Dok"
    table="test"
    primary_keys=["id"]
    schema={
        fields {
            id = string
            name = string
        }
    }
  }
}


sink {
  MongoDB{
    uri = "mongodb://localhost:27017"
    database = "test"
    collection = "test"
    primary-key = ["id"]
    schema = {
      fields {
        id = string
        name = string
      }
    }
  }
}
```

