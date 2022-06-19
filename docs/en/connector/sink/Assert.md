# Assert

> # Sink plugin: Assert [Flink]

## Description

A flink sink plugin which can assert illegal data by user defined rules

:::tip

Engine Supported and plugin name

* [ ] Spark
* [x] Flink: AssertSink

:::

## Options

| name                    | type     | required | default value |
| ----------------------- | -------- | -------- | ------------- |
| rules         		  | ConfigList | yes      | -             |
| &ensp;field_name        | `String` | yes       | -     |
| &ensp;field_type        | `String` | no       | -          |
| &ensp;field_value | ConfigList | no       | -             |
| &ensp;&ensp;rule_type         | `String`    | no       | -             |
| &ensp;&ensp;rule_value         | double    | no       | -             |


### rules

Rule definition of user's available data.  Each rule represents one field validation.

### field_name

field name（string）

### field_type

field type (string),  e.g. `string,boolean,byte,short,int,long,float,double,char,void,BigInteger,BigDecimal,Instant`

### field_value

A list value rule define the data value validation

### rule_type

The following rules are supported for now
`
NOT_NULL,  
MIN,  
MAX,  
MIN_LENGTH,  
MAX_LENGTH
`

### rule_value

the value related to rule type


## Example
the whole config obey with `hocon` style

```hocon
AssertSink {
    rules = 
        [{
            field_name = name
            field_type = string
            field_value = [
                {
                    rule_type = NOT_NULL
                },
                {
                    rule_type = MIN_LENGTH
                    rule_value = 3
                },
                {
                     rule_type = MAX_LENGTH
                     rule_value = 5
                }
            ]
        },{
            field_name = age
            field_type = int
            field_value = [
                {
                    rule_type = NOT_NULL
                },
                {
                    rule_type = MIN
                    rule_value = 10
                },
                {
                     rule_type = MAX
                     rule_value = 20
                }
            ]
        }
        ]
    
}

```
