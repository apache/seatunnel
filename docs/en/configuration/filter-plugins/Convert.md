## Filter plugin : Convert

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Convert a field’s value to a different type, such as converting a string to an integer.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [new_type](#new_type-string) | string | yes | - |
| [source_field](#source_field-string) | string | yes | - |

##### new_type [string]

Conversion type, supports `string`, `integer`, `long`, `float`, `double` and `boolean` now.

##### source_field [string]

Source field.


### Examples

```
convert {
    source_field = "age"
    new_type = "integer"
}
```

> Convert the `age` field to `integer` type


