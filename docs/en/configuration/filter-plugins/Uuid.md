## Filter plugin : Uuid

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Using Spark function `monotonically_increasing_id()` to add a globally unique and auto incrementing UUID field.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [target_field](#target_field-string) | string | no | uuid |

##### target_field [string]

New field name, default is `uuid`.

### Example

```
uuid {
    target_field = "id"
}
```