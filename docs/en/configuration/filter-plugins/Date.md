## Filter plugin : Date

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

The date filter is used for parsing dates from specified field.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [default_value](#default_value-string) | string | no | ${now} |
| [locale](#locale-string) | string | no | Locale.US |
| [source_field](#source_field-string) | string | no | \_\_root\_\_ |
| [source_time_format](#source_time_format-string) | string | no | UNIX_MS |
| [target_field](#target_field-string) | string | no | datetime |
| [target_time_format](#target_time_format-string) | string | no | `yyyy/MM/dd HH:mm:ss` |
| [time_zone](#time_zone-string) | string | no | - |

##### default_value [string]

If the date conversion fails, the current time(`${now}`) will be used in the specified format.


##### locale [string]

Locale of source field.

##### source_field [string]

Source field, if not configured, the current time will be used.

##### source_time_format [string]

Source field time format, currently supports UNIX(10-bit seconds timestamp), UNIX_MS(13-bit millisecond timestamp) and `SimpleDateFormat` format. The commonly used time formats are listed below:


| Symbol | Description |
| --- | --- |
| y | Year |
| M | Month |
| d | Day of month |
| H | Hour in day (0-23) |
| m | Minute in hour |
| s | Second in minute |

The detailed time format syntax:[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).



##### target_field [string]

Target field, default is `datetime`.

##### target_time_format [string]

Target field time format, The detailed time format syntax:[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).


##### time_zone [string]

Timezone of source field


### Examples

```
date {
    source_field = "timestamp"
    target_field = "date"
    source_time_format = "UNIX"
    target_time_format = "yyyy/MM/dd"
}
```

> Convert the `timestamp` field from UNIX timestamp to the `yyyy/MM/dd` format.

```
date {
    source_field = "httpdate"
    target_field = "datetime"
    source_time_format = "dd/MMM/yyyy:HH:mm:ss Z"
    target_time_format = "yyyy/MM/dd HH:mm:ss"
}
```


> Convert the `httpdate` field from `dd/MMM/yyyy:HH:mm:ss Z` format to the `yyyy/MM/dd HH:mm:ss` format
