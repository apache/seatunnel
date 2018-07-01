## Filter plugin : Drop

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Drop Rows that match the condition


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [condition](#condition-string) | string | yes | - |

##### condition [string]

Conditional expression, Rows that match this conditional expression will be dropped. Expressions in where clause of sql language can be used, such as `name = 'grayelephant'`, `status = 200 AND resp_time > 100`


### Examples

```
drop {
    condition = "status = '200'"
}
```

> Rows will be dropped if status is 200
