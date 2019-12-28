## Filter plugin : Grok

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

使用Grok Pattern来解析字段，[支持的grok pattern](https://github.com/InterestingLab/waterdrop/blob/master/plugins/grok/files/grok-patterns/grok-patterns),
 
grok pattern[grok pattern 测试地址](https://grokdebug.herokuapp.com/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [named_captures_only](#named_captures_only-boolean) | boolean | no | true |
| [pattern](#pattern-string) | string | yes | - |
| [patterns_dir](#patterns_dir-string) | string | no | - |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | __root__ |
| [common-options](#common-options-string)| string | no | - |


##### named_captures_only [boolean]

If true, only store named captures from grok.

##### pattern [string]

用于处理数据的grok pattern.

##### patterns_dir [string]

patterns文件路径，可不填，Waterdrop自带了丰富的[grok-patterns文件](https://github.com/InterestingLab/waterdrop/tree/master/plugins/grok/files/grok-patterns)

##### source_field [string]

数据源字段

##### target_field [string]

目标字段

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


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
