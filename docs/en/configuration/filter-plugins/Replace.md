## Filter plugin : Lowercase

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Replaces field contents based on grok.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [pattern](#pattern-string) | string | yes | - |
| [replacement](#replacement-string) | string | yes | - |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | replaced |

##### pattern [string]

Grok pattern.

##### replacement [string]

String need to be replaced.

##### source_field [string]

Source field, default is `raw_message`.

##### target_field [string]

New field name, default is `replaced`.

### Examples

```
replace {
    target_field = "tmp"
    source_field = "message"
    pattern = "is"
    replacement = "are"
}
```

> Replace **is** in `message` with **are** and set it to `tmp`.
