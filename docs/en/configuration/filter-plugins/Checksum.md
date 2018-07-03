## Filter plugin : Checksum

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Get the checksum of field.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [method](#method-string) | string | no | SHA1 |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | checksum |

##### method [string]

Checksum algorithm, supports SHA1,MD5 and CRC32 now.

##### source_field [string]

Source field

##### target_field [string]

Target field

### Examples

```
checksum {
    source_field = "deviceId"
    target_field = "device_crc32"
    method = "CRC32"
}
```

> Get CRC32 checksum from `deviceId`, and set it to `device_crc32`
