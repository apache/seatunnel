## Filter plugin : Lowercase

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Lowercase specified string field.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | lowercased |

##### source_field [string]

Source field, default is `raw_message`

##### target_field [string]

New field name, default is `lowercased`

# Examples

```
lowercase {
    source_field = "address"
    target_field = "address_lowercased"
}
```
