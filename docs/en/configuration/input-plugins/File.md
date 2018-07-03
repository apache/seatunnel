## Input plugin : File

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Read raw data from local file

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |

##### path [string]

File path.

### Example

```
file {
    path = "file:///var/log/access.log"
}
```
