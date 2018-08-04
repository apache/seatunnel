## Filter plugin : Truncate

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Truncate string.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [max_length](#max_length-number) | number | no | 256 |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | truncated |

##### max_length [number]

Maximum length of the string.

##### source_field [string]

Source field name, default is `raw_message`.

##### target_field [string]

New field name, default is `__root__`.

### Example

```
truncate {
    source_field = "telephone"
    max_length = 10
}
```
