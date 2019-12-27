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

##### schema_dir [string]

Json schema absolute directory path，default is `$WaterdropRoot/plugins/json/files/schemas/`

##### schema_file [string]

Json schema file name, if not set, the system will infer the schema from input source.

### Use cases

1. `json schema` **use case**

There might be multiple input json schemas in a single job, e.g. the schema in topicA of kafka can be:

```json
{
  "A": "a_val",
  "B": "b_val"
}
```

the schema of topicB can be:

```json
{
  "C": "c_val",
  "D": "d_val"
}
```

If we need to combine two schemas and make it output as a wide table, we can specify a schema with content below:

```json
{
  "A": "a_val",
  "B": "b_val",
  "C": "c_val",
  "D": "d_val"
}
```

then the output of topicA and topicB would be：

```
+-----+-----+-----+-----+
|A    |B    |C    |D    |
+-----+-----+-----+-----+
|a_val|b_val|null |null |
|null |null |c_val|d_val|
+-----+-----+-----+-----+
```


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

3. With `schema_file`
    ```
    json {
        source_field = "message"
        schema_file = "demo.json"
    }
    ```
    
    * **Schema**
    
    Make the content of `/opt/waterdrop/plugins/json/files/schemas/demo.json` on `Driver node` as below：
    
    ```json
    {
       "name": "demo",
       "age": 24,
       "city": "LA"
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
    +----------------------------+---+-----+-----+
    |message                     |age|name |city |
    +----------------------------+---+-----+-----+
    |{"name": "gary", "age": 28} |28 |gary |null |
    |{"name": "ricky", "age": 23}|23 |ricky|null |
    +----------------------------+---+-----+-----+
    ```

    > If deploy in `cluster` mode，make sure json schemas directory is packed in plugins.tar.gz