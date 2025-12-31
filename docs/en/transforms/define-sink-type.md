# Define Sink Type

> Define sink type transform plugin

## Description

Used to define the storage type of sink field. This is effective when the savemode enables automatic table creation.

## Options

|  name   | type                      | required | default value | Description                                                            |
|:-------:|---------------------------|----------|---------------|------------------------------------------------------------------------|
| columns | list<map<string, string>> | yes      |               | The columns to be defined, the name and type of the column must be set |

## Examples

### Define sink columns type for savemode

```
transform {
  DefineSinkType {
    columns = [
        {
            column = "c1"
            type = "nvarchar2(10)"
        }
        {
            column = "c2"
            type = "datetime(6)"
        }
        {
            column = "c3"
            type = "your target type"
        }
    ]
  }
}
```
