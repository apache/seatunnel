## Output plugin : Stdout

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Output Rows to console.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [limit](#limit-number) | number | no | 100 |
| [serializer](#serializer-string) | string | no | plain |

##### limit [number]

Limit number of output. `-1` means no limit.

##### serializer [string]

The serializer used for output.[Serializers](/#/zh-cn/)

### Example

```
stdout {
    limit = 10
    serializer = "json"
}
```
