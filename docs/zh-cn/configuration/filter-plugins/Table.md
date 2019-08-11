## Filter plugin : Table

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Table 用于将静态文件映射为一张表，可与实时处理的流进行关联，常用于用户昵称，国家省市等字典表关联

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [cache](#cache-boolean) | boolean | no | true |
| [delimiter](#delimiter-string) | string | no | , |
| [field_types](#field_types-array) | array | no | - |
| [fields](#fields-array) | array | yes | - |
| [path](#path-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### cache [boolean]

是否内存中缓存文件内容，true表示缓存，false表示每次需要时重新加载

##### delimiter [string]

文件中列与列之间的分隔符

##### field_types [array]

每个列的类型，顺序与个数必须与`fields`参数一一对应, 不指定此参数，默认所有列的类型为字符串; 支持的数据类型包括：boolean, double, long, string

##### fields [array]

文件中，每行中各个列的名称，按照数据中实际列顺序提供

##### path [string]

Hadoop支持的文件路径(默认hdfs路径, 如/path/to/file), 如本地文件：file:///path/to/file, hdfs:///path/to/file, s3:///path/to/file ...

##### table_name [string]

将文件载入后将注册为一张表，这里指定的是表名称，可用于在SQL中直接与流处理数据关联

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Example

> 不指定列的类型，默认为string

```
table {
    table_name = "mydict"
    path = "/user/waterdrop/mylog/a.txt"
    fields = ['city', 'population']
}
```

> 指定列的类型

```
table {
    table_name = "mydict"
    path = "/user/waterdrop/mylog/a.txt"
    fields = ['city', 'population']
    field_types = ['string', 'long']
}
```
