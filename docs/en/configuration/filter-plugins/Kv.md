## Filter plugin : Kv

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Extract all Key-Values of the specified field, which are often used to parse the url parameter.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [exclude_fields](#exclude_fields-array) | array | no | [] |
| [field_prefix](#field_prefix-string) | string | no |  |
| [field_split](#field_split-string) | string | no | & |
| [include_fields](#include_fields-array) | array | no | [] |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | \_\_root\_\_ |
| [value_split](#value_split-string) | string | no | = |

##### exclude_fields [array]

An array specifying the parsed keys which should not be added to the Rows.

##### field_prefix [string]

A string to prepend to all of the extracted keys.

##### field_split [string]

A string of characters to use as single-character field delimiters for parsing out key-value pairs.


##### include_fields [array]

An array specifying the parsed keys which should be added to the event.

##### source_field [string]

Source field.

##### target_field [string]

New field name.

##### value_split [string]

A non-empty string of characters to use as single-character value delimiters for parsing out key-value pairs.

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
