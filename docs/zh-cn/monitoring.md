# Guardian

Guardian是基于Yarn上的、可配置的Spark Application监控系统。可以提供Spark应用的存活情况监控以及调度延迟情况监控，并提供RESTFUL API支持配置的实时修改

## 使用方式

> python guardian.py check config.json

## 接口使用方式介绍

Guardian支持通过接口对config.json文件实时修改

### GET

```
GET localhost:5000/config/waterdrop
```

获取配置文件中`app_name`为waterdrop的配置信息

### POST

```
POST localhost:5000/config/waterdrop -d '
{
    "active": false
}
'
```

将配置文件中`app_name`为waterdrop的`active`置为false。若`app_name`为waterdrop的配置不存在，则新建一个`app_name`为waterdrop的配置，其`active`为false。


### DELETE

```
DELETE localhost:5000/config/waterdrop
```

将配置文件中`app_name`为waterdrop的配置删除