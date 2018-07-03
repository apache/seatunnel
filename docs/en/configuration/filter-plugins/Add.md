## Filter plugin : Add

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Add a field to Rows.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [target_field](#target_field-string) | string | yes | - |
| [value](#value-string) | string | yes | - |

##### target_field [string]

New field name.

##### value [string]

New field value.

### Examples

```
add {
    value = "1"
}
```

> Add a field, the value is "1"
