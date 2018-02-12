## Filter plugin : Grok

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对指定字段进行正则解析，grok插件[测试地址](https://grokdebug.herokuapp.com/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [named_captures_only](#named_captures_only-boolean) | boolean | no | true |
| [pattern](#pattern-string) | string | yes | - |
| [patterns_dir](#patterns_dir-string) | string | no | - |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | __root__ |

##### named_captures_only [boolean]

If true, only store named captures from grok.

##### pattern [string]

正则表达式

##### patterns_dir [string]

patterns文件路径，可不填，项目里准备了丰富[grok-patterns文件](https://github.com/InterestingLab/waterdrop/tree/master/plugins/grok/files/grok-patterns)

##### source_field [string]

数据源字段

##### target_field [string]

目标字段

### Example

```
grok {
    source_field = "raw_message"
    pattern = "%{WORD:name} is %{WORD:gender}, %{NUMBER:age} years old and weighs %{NUMBER:weight} kilograms"
    target_field = "info_detail"
}
```

* **Input**

```
+----------------------------------------------------+
|raw_message                                         |
+----------------------------------------------------+
|gary is male, 25 years old and weighs 68.5 kilograms|
|gary is male, 25 years old and weighs 68.5 kilograms|
+----------------------------------------------------+
```

* **Output**

```
+----------------------------------------------------+------------------------------------------------------------+
|raw_message                                         |info_detail                                                 |
+----------------------------------------------------+------------------------------------------------------------+
|gary is male, 25 years old and weighs 68.5 kilograms|Map(age -> 25, gender -> male, name -> gary, weight -> 68.5)|
|gary is male, 25 years old and weighs 68.5 kilograms|Map(age -> 25, gender -> male, name -> gary, weight -> 68.5)|
+----------------------------------------------------+------------------------------------------------------------+

```

> grok处理的结果支持**select * from where info_detail.age = 25**此类SQL语句