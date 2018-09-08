## Filter plugin : Script

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

解析并执行自定义脚本中逻辑

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [json_name](#json_name-string) | string | no | value |
| [script_name](#script_name-string) | string | yes | - |
| [errorList](#errorList-boolean) | boolean | no | false |
| [isCache](#isCache-boolean) | boolean | no | false |
| [isTrace](#isTrace-boolean) | boolean | no | false |
| [isPrecise](#isPrecise-boolean) | boolean | no | false |

##### json_name [string]

脚本内置JSONObject的引用名

##### script_name [string]

需要执行脚本的名称

##### errorList [boolean]

输出的错误信息List

##### isCache [boolean]

是否使用Cache中的指令集

##### isTrace [boolean]

是否输出所有的跟踪信息，同时还需要log级别是DEBUG级

##### isPrecise [boolean]

是否需要高精度的计算


### Examples

* conf文件插件配置
```
  script {
    json_name = "myJson"
    script_name = "my_script.ql"
  }
```
* 自定义脚本(my_script.ql)
```
event = new java.util.HashMap();
you = myJson.getString("name");
age = myJson.getLong("age");
if(age > 10){
event.put("name",you);
}
return event;
```

> 如果age大于10，则获取name放入map中并返回
