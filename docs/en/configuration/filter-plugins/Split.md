## Filter plugin : Split

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Splits String using delimiter.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [delimiter](#delimiter-string) | string | no | " "(Space) |
| [fields](#fields-list) | list | yes | - |
| [source_field](#source_field-string) | string | no | _root_ |
| [target_field](#target_field-string) | string | no | raw_message |

##### delimiter [string]

The string to split on. Default is a space.


##### fields [list]

The divided list of field names. People need specify the field names of the divided strings in order.

If the length of `fields` is greater than the length of divided result, the extra fields will be set to empty string.

##### source_field [string]

Source field, default is `raw_message`.

##### target_field [string]

New field name, default is `__root__`, and the result of `Split` will be added on the top level of Rows.

If you specify `target_field`, the result of 'Split' will be added under the top level of Rows.

### Examples

```
split {
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> The string of `message` is split by **&**, and the results set to `field1` and `field2`.

```
split {
    source_field = "message"
    target_field = "info"
    delimiter = ","
    fields = ["field1", "field2"]
}
```

> The string of `message` is split by **&**, and the results set to `info.field1` and `info.field2`

