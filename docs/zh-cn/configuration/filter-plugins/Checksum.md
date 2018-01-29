## Filter plugin : Checksum

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

获取指定字段的校验码

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [method](#method-string) | string | no | SHA1 |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | checksum |

##### method [string]

校验方法，当前支持SHA1、MD5和CRC32

##### source_field [string]

源字段

##### target_field [string]

转换后的字段

### Examples

```
checksum {
    source_field = "deviceId"
    target_field = "device_crc32"
    method = "CRC32"
}
```

> 获取`deviceId`字段CRC32校验码
