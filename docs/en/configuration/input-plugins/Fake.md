## Input plugin : Fake

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

Input plugin for producing test data.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [data_format](#data_format-string) | string | no | text |
| [json_keys](#json_keys-array) | array | no | - |
| [num_of_fields](#num_of_fields-number) | number | no | 10 |
| [rate](#rate-number) | number | yes | - |
| [text_delimeter](#text_delimeter-string) | string | no | , |

##### data_format [string]

The format of test data, supports `text` and `json`

##### json_keys [array]

The list of JSON data's key, used when `data_format` is json

##### num_of_fields [number]

The number of fields, used when `data_format` is text


##### rate [number]

The number of test data produced per second

##### text_delimiter [string]

Text data separator, used when `data_format` is text

### Examples

1. With `data_format`

    ```
    fake {
        data_format = "text"
        text_delimeter = ","
        num_of_fields = 5
        rate = 5
    }
    ```

* **Input**

    ```
    +-------------------------------------------------------------------------------------------+
    |raw_message                                                                                |
    +-------------------------------------------------------------------------------------------+
    |Random1-1462437280,Random215896330,Random3-2009195549,Random41027365838,Random51525395111  |
    |Random1-2135047059,Random2-1030689538,Random3-854912064,Random4126768642,Random5-1483841750|
    +-------------------------------------------------------------------------------------------+
    ```


2. Without `data_format`

    ```
    fake {
        content = ['name=ricky&age=23', 'name=gary&age=28']
        rate = 5
    }
    ```

* **Input**

    ```
    +-----------------+
    |raw_message      |
    +-----------------+
    |name=gary&age=28 |
    |name=ricky&age=23|
    +-----------------+
    ```

    > Randomly extract the string from the `content ` list
