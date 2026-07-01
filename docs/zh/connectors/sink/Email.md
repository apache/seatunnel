import ChangeLog from '../changelog/connector-email.md';

# Email

> Email 数据接收器

## 描述

将接收到的数据写成附件文件，并发送到一个或多个邮箱地址。

## 支持版本

测试版本:1.5.6(供参考)

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## 选项

|            名称            |   类型    | 是否必须 | 默认值 |
|--------------------------|---------|------|-----|
| email_from_address       | string  | 是    | -   |
| email_to_address         | string  | 是    | -   |
| email_host               | string  | 是    | -   |
| email_transport_protocol | string  | 是    | -   |
| email_smtp_auth          | boolean | 是    | -   |
| email_smtp_port          | int     | 否    | 465           |
| email_authorization_code | string  | 是    | -             |
| email_message_headline   | string  | 是    | -             |
| email_message_content    | string  | 是    | -             |
| email_attachment_name    | string  | 否    | emailsink.csv |
| email_field_delimiter    | string  | 否    | ,             |
| common-options           |         | 否    | -             |

### email_from_address [string]

发件人邮箱地址

### email_to_address [string]

接收邮件的地址，支持多个邮箱地址，以逗号（,）分隔。

示例：`receiver-1@example.com,receiver-2@example.com`。

### email_host [string]

连接的SMTP服务器地址

### email_transport_protocol [string]

加载会话的协议

### email_smtp_auth [boolean]

是否对客户进行认证

### email_smtp_port [int]

选择用于身份验证的端口。

### email_authorization_code [string]

授权码或密码，可以从邮箱设置中获取。

连接器要求必须配置该项。当 `email_smtp_auth = false` 时，可以配置为空字符串。

### email_message_headline [string]

邮件的标题

### email_message_content [string]

邮件消息的正文

### email_attachment_name [string]

邮件附件的文件名。默认为 `emailsink.csv`。连接器会先把数据写到本地这个文件里，再作为附件发送。

### email_field_delimiter [string]

附件文件中用于分隔字段的分隔符。默认为逗号 `,`。

附件不包含表头。字段会按上游 schema 的顺序写入，`null` 值会写成空字符串。

### common options

Sink插件常用参数，请参考 [Sink常用选项](../common-options/sink-common-options.md) 了解详情.

## 示例

### 发送单表数据到多个收件人

这个示例来自 Email e2e 任务。示例使用不需要认证的测试 SMTP 服务，并给 `email_to_address`
中的每个邮箱各发送一封邮件。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        row.num = 100
        schema = {
          table = "test.table1"
          columns = [
            {
              name = "id"
              type = "bigint"
            },
            {
              name = "name"
              type = "string"
            },
            {
              name = "age"
              type = "int"
            }
          ]
        }
      }
    ]
  }
}

sink {
  EmailSink {
    email_from_address = "sender@example.com"
    email_to_address = "receiver-1@example.com,receiver-2@example.com"
    email_host = "email-e2e"
    email_transport_protocol = "smtp"
    email_smtp_auth = false
    email_smtp_port = 3025
    email_authorization_code = ""
    email_message_headline = "test-title"
    email_message_content = "test-content"
    email_attachment_name = "report.csv"
    email_field_delimiter = "|"
  }
}
```

### 发送多表数据

Email sink 支持多表输入。在 e2e 任务中，两个上游表会让每个收件人收到两封邮件。

```hocon
source {
  FakeSource {
    tables_configs = [
      {
        row.num = 100
        schema {
          table = "test.table1"
          fields {
            id = bigint
            name = string
            age = int
          }
        }
      },
      {
        row.num = 100
        schema {
          table = "test.table2"
          fields {
            id = bigint
            name = string
            age = int
          }
        }
      }
    ]
  }
}

sink {
  EmailSink {
    email_from_address = "sender@example.com"
    email_to_address = "receiver-3@example.com,receiver-4@example.com"
    email_host = "email-e2e"
    email_transport_protocol = "smtp"
    email_smtp_auth = false
    email_smtp_port = 3025
    email_authorization_code = ""
    email_message_headline = "test-title"
    email_message_content = "test-content"
  }
}
```

## 变更日志

<ChangeLog />
