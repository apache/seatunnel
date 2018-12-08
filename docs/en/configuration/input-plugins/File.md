## Input plugin : File

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.1

### Description

Read raw data from local file system.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | string | yes | json |
| [path](#path-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |

##### format [string]

The input data source format.

##### path [string]

File path.

##### table_name [string]

Registered table name of input data.

### Example

```
file {
    path = "file:///var/log/access.log"
    table_name = "access"
    format = "text"
}
```
