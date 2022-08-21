# Nulltf

> NULL default value transform plugin

## Description

set default value for null field

:::tip

This transform only supported by engine Spark.

:::

## Options

| name                | type    | required | default value |
| ------------------- | ------- | -------- | ------------- |
| fields              | array   | no       | -             |

### fields [list]

A list of fields whose default value will be set. 
The default value of the field can be set in the form of "field:value", If no set, the default value will be set according to the field type.

## Examples

the configuration

```bash
  nulltf {
      fields {
          name: "",
          price: 0,
          num: 100,
          flag: false,
          dt_timestamp: "2022-05-18 13:51:40.603",
          dt_date: "2022-05-19"
      }
  }
```

before use nulltf transform

```bash
+-----+-----+----+-----+--------------------+----------+
| name|price| num| flag|        dt_timestamp|   dt_date|
+-----+-----+----+-----+--------------------+----------+
|名称1| 22.5| 100|false|2022-05-20 14:34:...|2022-05-20|
| null| 22.5| 100|false|2022-05-20 14:35:...|2022-05-20|
|名称1| null| 100|false|2022-05-20 14:35:...|2022-05-20|
|名称1| 22.5|null|false|2022-05-20 14:36:...|2022-05-20|
|名称1| 22.5| 100| null|2022-05-20 14:36:...|2022-05-20|
|名称1| 22.5| 100|false|                null|2022-05-20|
|名称1| 22.5| 100|false|2022-05-20 14:37:...|      null|
+-----+-----+----+-----+--------------------+----------+
```

after use nulltf transform

```bash
+-----+-----+----+-----+--------------------+----------+
| name|price| num| flag|        dt_timestamp|   dt_date|
+-----+-----+----+-----+--------------------+----------+
|名称1| 22.5|100|false|2022-05-20 14:34:...|2022-05-20|
|     | 22.5|100|false|2022-05-20 14:35:...|2022-05-20|
|名称1|  0.0|100|false|2022-05-20 14:35:...|2022-05-20|
|名称1| 22.5|100|false|2022-05-20 14:36:...|2022-05-20|
|名称1| 22.5|100|false|2022-05-20 14:36:...|2022-05-20|
|名称1| 22.5|100|false|2022-05-18 13:51:...|2022-05-20|
|名称1| 22.5|100|false|2022-05-20 14:37:...|2022-05-19|
+-----+-----+---+-----+--------------------+----------+
```


