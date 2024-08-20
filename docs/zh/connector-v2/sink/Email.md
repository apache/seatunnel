# Email

> Email 数据接收器

## 描述

将接收的数据作为文件发送到电子邮件

## 支持版本

测试版本:1.5.6(供参考)

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|            名称            |   类型    | 是否必须 | 默认值 |
|--------------------------|---------|------|-----|
| email_from_address       | string  | 是    | -   |
| email_to_address         | string  | 是    | -   |
| email_host               | string  | 是    | -   |
| email_transport_protocol | string  | 是    | -   |
| email_smtp_auth          | boolean | 是    | -   |
| email_smtp_port          | int     | 否    | 465 |
| email_authorization_code | string  | 否    | -   |
| email_message_headline   | string  | 是    | -   |
| email_message_content    | string  | 是    | -   |
| common-options           |         | 否    | -   |

### email_from_address [string]

发件人邮箱地址

### email_to_address [string]

接收邮件的地址，支持多个邮箱地址，以逗号（,）分隔。

### email_host [string]

连接的SMTP服务器地址

### email_transport_protocol [string]

加载会话的协议

### email_smtp_auth [boolean]

是否对客户进行认证

### email_smtp_port [int]

选择用于身份验证的端口。

### email_authorization_code [string]

授权码,您可以从邮箱设置中获取授权码

### email_message_headline [string]

邮件的标题

### email_message_content [string]

邮件消息的正文

### common options

Sink插件常用参数，请参考 [Sink常用选项](../sink-common-options.md) 了解详情.

## 示例

```bash

 EmailSink {
      email_from_address = "xxxxxx@qq.com"
      email_to_address = "xxxxxx@163.com"
      email_host="smtp.qq.com"
      email_transport_protocol="smtp"
      email_smtp_auth="true"
      email_authorization_code=""
      email_message_headline=""
      email_message_content=""
   }

```

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加 Email 接收器连接器

