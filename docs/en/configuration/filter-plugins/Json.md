## Filter plugin : Json

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

It takes an existing field which contains a json string and extract its fields.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | \_\_root\_\_ |

##### source_field [string]

Source field, default is `raw_message`.

##### target_field [string]

New field name.

### Examples

1. Without `target_field`

    ```
    json {
        source_field = "message"
    }
    ```

    * **Input**

    ```
    +----------------------------+
    |message                   |
    +----------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28} |
    +----------------------------+
    ```

    * **Output**

    ```
    +----------------------------+---+-----+
    |message                   |age|name |
    +----------------------------+---+-----+
    |{"name": "gary", "age": 28} |28 |gary |
    |{"name": "ricky", "age": 23}|23 |ricky|
    +----------------------------+---+-----+
    ```

2. With `target_field`

    ```
    json {
        source_field = "message"
        target_field = "info"
    }
    ```

    * **Input**

    ```
    +----------------------------+
    |message                   |
    +----------------------------+
    |{"name": "ricky", "age": 24}|
    |{"name": "gary", "age": 28} |
    +----------------------------+
    ```

    * **Output**

    ```
    +----------------------------+----------+
    |message                   |info      |
    +----------------------------+----------+
    |{"name": "gary", "age": 28} |[28,gary] |
    |{"name": "ricky", "age": 23}|[23,ricky]|
    +----------------------------+----------+

    ```
