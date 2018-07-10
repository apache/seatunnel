## Filter plugin : Remove

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Remove all specified field from Rows.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-array) | array | yes | - |

##### source_field [array]

Array of fields needed to be removed.

### Examples

```
remove {
    source_field = ["field1", "field2"]
}
```

> Remove `field1` and `field2` from Rows.
