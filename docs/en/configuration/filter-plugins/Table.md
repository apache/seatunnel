## Filter plugin : Table

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

It is used to map static files into a table, which can be associated with real-time processed streams. It is always used for user nicknames, national provinces and cities, etc.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [cache](#cache-boolean) | boolean | no | true |
| [delimiter](#delimiter-string) | string | no | , |
| [field_types](#field_types-array) | array | no | - |
| [fields](#fields-array) | array | yes | - |
| [path](#path-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |

##### cache [boolean]

Whether to cache file contents in memory. If false, it will reload every time you need.

##### delimiter [string]

The delimiter between columns in the file.

##### field_types [array]

The type of each field, the order and length of `field_types` must correspond to the `fields` parameter. The default type of all columns is string. Supported data types include: `boolean`, `double`, `long`, `string`

##### fields [array]

The names of the columns in each row, while should be provided by the actual columns in the data in order.

##### path [string]


File path supported by Spark. For example, file:///path/to/file, hdfs:///path/to/file, s3:///path/to/file ...

##### table_name [string]

After loading the file, it will be registered as a table. Here, the table name is specified, which can be used to directly associate with the stream processing data.


### Example

> Without `field_types`

```
table {
    table_name = "mydict"
    path = "/user/waterdrop/mylog/a.txt"
    fields = ['city', 'population']
}
```

> With `field_types`

```
table {
    table_name = "mydict"
    path = "/user/waterdrop/mylog/a.txt"
    fields = ['city', 'population']
    field_types = ['string', 'long']
}
```
