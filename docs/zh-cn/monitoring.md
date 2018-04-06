# Guardian

Guardian是Waterdrop的监控和报警工具，可以提供Waterdrop的存活情况监控以及调度延迟情况监控。Guardian能够在运行是动态加载配置文件，并提供HTTP API支持配置的实时修改。目前仅支持Waterdrop on Yarn.

## 配置文件

Guardian通过命令行和一个配置文件运行

> python guardian.py check config.json

这个配置文件使用`JSON`格式编写，一个有效的实例，点击[这里]()

整个配置文件由以下几个部分组成：

- node_name: 节点信息
- check_interval: 检测应用的时间间隔
- yarn: 被检测的YARN集群地址
- apps: 需要被检测的具体应用
- alert_manager: 报警管理




### yarn

```
# Yarn resourcemanager
api_hosts: <list>
```

**Example**

```
"yarn": {
    "api_hosts": [
        "10.11.10.21:8088",
        "10.11.10.22:8088"
    ]
}
```

### apps

```
[{
    # Spark application name
    "app_name": <string>,
    # 当应用失败时的重启命令
    "start_cmd": <string>,
    # 同一个app_name下的应用运行个数
    "app_num": <number>,
    # Application type, default 'spark'
    "check_type": <string>,
    # 标志这个应用是否有效
    "active": <boolean>
    "check_options": {
        # 报警级别，支持WARNNING、ERROR等
        "alert_level": <string>,
        "max_delayed_batch_num": <number>,
        "max_delayed_time": <number>
    }
}]
```

**Example**

```
"apps": [
    {
        "app_name": "waterdrop-app",
        "start_cmd": "test_cmd",
        "app_num": 1,
        "check_type": "spark",
        "check_options": {
            "alert_level": "WARNING",
            "max_delayed_batch_num": 10,
            "max_delayed_time": 600
        }
    }
]
```

### alert_manager

#### routes

报警路由，当前仅支持报警级别

当报警级别为`WARNNING`或`ERROR`触发报警

```
"routes": {
    "match": {
        "level": ["WARNING", "ERROR"]
    }
}
```

#### \<emails>

通过邮件发送报警信息

```
# 邮箱验证用户名
"auth_username": <string>,
# 邮箱验证密码
"auth_password": <string>,
# 邮箱stmp服务器
"smtp_server": <string>,
# 发件人
"sender": <string>,
# 收件人列表
"receivers": <list>
```

**Example**

```
"emails": {
    "auth_username": "username",
    "auth_password": "password",
    "smtp_server": "smtp.163.com",
    "sender": "huochen1994@163.com",
    "receivers": ["garygaowork@gmail.com"],
    "routes": {
        "match": {
            "level": ["WARNING", "ERROR"]
        }
    }
}
```

#### \<webhook>

通过接口实现自定义报警方式

```
# webhook接口地址
"url": <string>
```

**Example**

```
"webhook": {
    "url": "http://api.webhook.interestinglab.org/alert",
    "routes": {
        "match": {
            "level": ["ERROR"]
        }
    }
}
```

Gaurdian调用接口的时候会以下面JSON格式发送HTTP POST请求到配置的接口地址：

```
{
    "subject": "Guardian",
    "objects": "Waterdrop_app",
    "content": "App is not running or less than expected number of running instance, will restart"
}
```


## Guardian接口使用指南


### GET

#### 概述

* 功能描述

    获取Guardian对应app_name的配置信息

* 基础接口

    http://localhost:5000/config/<app_name>

* 请求方式

    get

#### 接口参数定义

N/A

#### 返回结果

```
curl 'http://localhost:5000/config/waterdrop-app2'

{
  "content": {
    "app_name": "waterdrop-app2",
    "app_num": 1,
    "check_options": {},
    "check_type": "spark",
    "start_cmd": "test_cmd_not_exist"
  },
  "status": 0
}
```

### POST

#### 概述

* 功能描述

    更新或新增Guardian中应用配置信息，当`app_name`存在，更新对应配置信息，当`app_name`不存在，新增一个应用监控配置

* 基础接口

    http://localhost:5000/config/<app_name>

* 请求方式

    post

#### 接口参数定义

| 字段 | 类型 | 注释 | 实例 |
| :--: | :--: | :--: | :--:|
| start_cmd| string| 重启命令|  |
|app_num| num | 存在个数 | 2 |
|check_type| string | 应用类型 | spark |
|check_options| dict| | |
|active| boolean| 是否有效| true|

#### 返回结果

```
curl 'http://localhost:5000/config/waterdrop-app2' -d '
{
    'active': false
}'

{
  "status": 0
}
```

### DELETE

#### 概述

* 功能描述

    删除Guardian对应app_name的配置信息

* 基础接口

    http://localhost:5000/config/<app_name>

* 请求方式

    delete

#### 接口参数定义

N/A

#### 返回结果

```
curl -XDELETE 10.212.81.56:5000/config/waterdrop-app2

{
  "status": 0
}
```


### 返回状态码说明

| status | 说明 |
| :--: | :--:|
| 0 | 成功|
| 1 | 参数错误|
| 2 | 内部错误|
