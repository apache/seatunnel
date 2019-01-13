## Filter plugin : Kv

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Extract all Key-Values of the specified string field with configured `field_split`, which are often used to parse the url parameter.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [default_values](#default_values-array) | array | no | [] |
| [exclude_fields](#exclude_fields-array) | array | no | [] |
| [field_prefix](#field_prefix-string) | string | no |  |
| [field_split](#field_split-string) | string | no | & |
| [include_fields](#include_fields-array) | array | no | [] |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | \_\_root\_\_ |
| [value_split](#value_split-string) | string | no | = |

##### default_values [array]

Default values can be set by `default_values` by `key=defalut_value`(key and value are separated by `=`).

Multiple default values are specified as follows: `default_values = ["mykey1=123", "mykey2=waterdrop"]`

##### exclude_fields [array]

Fields in the `exclude_fields` will be abandoned.

##### field_prefix [string]

A string to prepend to all of the extracted keys.

##### field_split [string]

A string of characters to use as single-character field delimiters for parsing key-value pairs.

##### include_fields [array]

An array specifying the parsed keys which should be added to the event.

##### source_field [string]

Source field.

##### target_field [string]

All extracted fields will be put into `target_field`.

##### value_split [string]

A non-empty string of characters to use as single-character value delimiters for parsing key-value pairs.

### Examples

1. With `target_field`

    ```
    kv {
        source_field = "message"
        target_field = "kv_map"
        field_split = "&"
        value_split = "="
    }
    ```

    * **Input**

    ```
    +-----------------+
    |message         |
    +-----------------+
    |name=ricky&age=23|
    |name=gary&age=28 |
    +-----------------+
    ```

    * **Output**

    ```
    +-----------------+-----------------------------+
    |message          |kv_map                    |
    +-----------------+-----------------------------+
    |name=ricky&age=23|Map(name -> ricky, age -> 23)|
    |name=gary&age=28 |Map(name -> gary, age -> 28) |
    +-----------------+-----------------------------+
    ```


2. Without `target_field`

    ```
    kv {
            source_field = "message"
            field_split = "&"
            value_split = "="
        }
    ```

    * **Input**

    ```
    +-----------------+
    |message         |
    +-----------------+
    |name=ricky&age=23|
    |name=gary&age=28 |
    +-----------------+
    ```

    * **Output**

    ```
    +-----------------+---+-----+
    |message         |age|name |
    +-----------------+---+-----+
    |name=ricky&age=23|23 |ricky|
    |name=gary&age=28 |28 |gary |
    +-----------------+---+-----+

    ```
