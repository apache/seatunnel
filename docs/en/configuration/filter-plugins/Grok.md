## Filter plugin : Grok

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Parse arbitrary text and structure events using Grok Pattern.[Supported grok pattern](https://github.com/InterestingLab/waterdrop/blob/master/plugins/grok/files/grok-patterns/grok-patterns).

If you need help building patterns to match your logs, you will find the [http://grokdebug.herokuapp.com](http://grokdebug.herokuapp.com)



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

Grok pattern.

##### patterns_dir [string]

The directory of pattern files. Waterdrop ships by default with a bunch of [patterns]([grok-patterns文件](https://github.com/InterestingLab/waterdrop/tree/master/plugins/grok/files/grok-patterns)), so you don’t necessarily need to define this yourself unless you are adding additional patterns.


##### source_field [string]

Source field.

##### target_field [string]

New field name.

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
