## Filter plugin : Replace

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Replaces field contents based on regular expression.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [pattern](#pattern-string) | string | yes | - |
| [replacement](#replacement-string) | string | yes | - |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | replaced |

##### pattern [string]

regular expression, such as [a-z0-9], \w, \d

Regular expression used for matching string, such as `"[a-zA-Z0-9_-]+"`.Please see [Regex Pattern](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html) for details.

You can also go to [Regex 101](https://regex101.com/) to test your regex interactively.

##### replacement [string]

String replacement.

##### source_field [string]

Source field, default is `raw_message`.

##### target_field [string]

New field name, default is `replaced`.

### Examples

```
replace {
    target_field = "tmp"
    source_field = "message"
    pattern = "\w+"
    replacement = "are"
}
```

> Replace **\w+** in `message` with **are** and set it to `tmp` column.
