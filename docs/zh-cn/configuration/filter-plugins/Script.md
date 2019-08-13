## Filter plugin : Script

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.1

### Description

解析并执行自定义脚本中逻辑, 即接受`object_name`(默认是event) 指定的JSONObject,
完成自定义的处理逻辑，再返回一个新的event.

脚本解析引擎的实现，采用的是[QLExpress](https://github.com/alibaba/QLExpress), 
具体语法可参考[QLExpress 语法](https://github.com/alibaba/QLExpress#%E4%B8%89%E8%AF%AD%E6%B3%95%E4%BB%8B%E7%BB%8D).

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [object_name](#object_name-string) | string | no | events |
| [script_name](#script_name-string) | string | yes | - |
| [errorList](#errorList-boolean) | boolean | no | false |
| [isCache](#isCache-boolean) | boolean | no | false |
| [isTrace](#isTrace-boolean) | boolean | no | false |
| [isPrecise](#isPrecise-boolean) | boolean | no | false |
| [common-options](#common-options-string)| string | no | - |


##### object_name [string]

脚本内置JSONObject的引用名, 不设置默认为'event'

##### script_name [string]

需要执行脚本的文件名称, 注意脚本文件必须放到`plugins/script/files`目录下面.

##### errorList [boolean]

输出的错误信息List

##### isCache [boolean]

是否使用Cache中的指令集

##### isTrace [boolean]

是否输出所有的跟踪信息，同时还需要log级别是DEBUG级

##### isPrecise [boolean]

是否需要高精度的计算

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

* conf文件插件配置

```
  script {
    script_name = "my_script.ql"
  }
```

* 自定义脚本(my_script.ql)

```
newEvent = new java.util.HashMap();
you = event.getString("name");
age = event.getLong("age");
if(age > 10){
newEvent.put("name",you);
}
return newEvent;
```

> 如果age大于10，则获取name放入map中并返回
