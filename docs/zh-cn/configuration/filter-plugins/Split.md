## Filter plugin : Split

* Author: rickyhuo
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

根据delimiter对字符串拆分

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [delimiter](#delimiter-string) | string | yes | - |
| [fields](#fields-array) | array | yes | - |
| [source_field](#source_field-string) | string | no | - |
| [target_field](#target_field-string) | string | no | - |

##### delimiter [string]

分隔符

##### fields [array]

分割后的字段

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`ROOT`