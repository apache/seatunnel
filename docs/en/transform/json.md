# Json

> Json transform plugin

## Description

Json analysis of the specified fields of the original data set

:::tip

This transform **ONLY** supported by Spark.

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| source_field   | string | no       | raw_message   |
| target_field   | string | no       | __root__      |
| schema_dir     | string | no       | -             |
| schema_file    | string | no       | -             |
| common-options | string | no       | -             |

### source_field [string]

Source field, if not configured, the default is `raw_message`

### target_field [string]

The target field, if it is not configured, the default is `__root__` , and the result of Json parsing will be uniformly placed at the top of the `Dataframe`

### schema_dir [string]

Style directory, if not configured, the default is `$seatunnelRoot/plugins/json/files/schemas/`

### schema_file [string]

The style file name, if it is not configured, the default is empty, that is, the structure is not specified, and the system derives it by itself according to the input of the data source.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Schema Use cases

- `json schema` usage scenarios

The multiple data sources of a single task may contain different styles of json data. For example, the `topicA` style from `Kafka` is

```json
{
  "A": "a_val",
  "B": "b_val"
}
```

The style from `topicB` is

```json
{
  "C": "c_val",
  "D": "d_val"
}
```

When running `Transform` , you need to fuse the data of `topicA` and `topicB` into a wide table for calculation. You can specify a `schema` whose content style is:

```json
{
  "A": "a_val",
  "B": "b_val",
  "C": "c_val",
  "D": "d_val"
}
```

Then the fusion output result of `topicA` and `topicB` is:

```bash
+-----+-----+-----+-----+
|A    |B    |C    |D    |
+-----+-----+-----+-----+
|a_val|b_val|null |null |
|null |null |c_val|d_val|
+-----+-----+-----+-----+
```

## Examples

### Do not use `target_field`

```bash
json {
    source_field = "message"
}
```

- Source

```bash
+----------------------------+
|message                   |
+----------------------------+
|{"name": "ricky", "age": 24}|
|{"name": "gary", "age": 28} |
+----------------------------+
```

- Sink

```bash
+----------------------------+---+-----+
|message                   |age|name |
+----------------------------+---+-----+
|{"name": "gary", "age": 28} |28 |gary |
|{"name": "ricky", "age": 23}|23 |ricky|
+----------------------------+---+-----+
```

### Use `target_field`

```bash
json {
    source_field = "message"
    target_field = "info"
}
```

- Souce

```bash
+----------------------------+
|message                   |
+----------------------------+
|{"name": "ricky", "age": 24}|
|{"name": "gary", "age": 28} |
+----------------------------+
```

- Sink

```bash
+----------------------------+----------+
|message                   |info      |
+----------------------------+----------+
|{"name": "gary", "age": 28} |[28,gary] |
|{"name": "ricky", "age": 23}|[23,ricky]|
+----------------------------+----------+
```

> The results of json processing support `select * from where info.age = 23` such SQL statements

### Use `schema_file`

```bash
json {
    source_field = "message"
    schema_file = "demo.json"
}
```

- Schema

Place the following content in `~/seatunnel/plugins/json/files/schemas/demo.json` of Driver Node:

```json
{
   "name": "demo",
   "age": 24,
   "city": "LA"
}
```

- Source

```bash
+----------------------------+
|message                   |
+----------------------------+
|{"name": "ricky", "age": 24}|
|{"name": "gary", "age": 28} |
+----------------------------+
```

- Sink

```bash
+----------------------------+---+-----+-----+
|message                     |age|name |city |
+----------------------------+---+-----+-----+
|{"name": "gary", "age": 28} |28 |gary |null |
|{"name": "ricky", "age": 23}|23 |ricky|null |
+----------------------------+---+-----+-----+
```

> If you use `cluster mode` for deployment, make sure that the `json schemas` directory is packaged in `plugins.tar.gz`
