import ChangeLog from '../changelog/connector-email.md';

# Email

> Email 数据接收器

## 描述

将接收到的数据写成附件文件，并发送到一个或多个邮箱地址。

连接器会把每张表的数据缓冲到一个带分隔符的附件文件里（每行一条记录，不含表头），并在 Writer 关闭时为每张表各发送一封邮件。如果某张表没有数据，则不会为该表发送邮件。

## 支持版本

测试版本:1.5.6(供参考)

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

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
| multi_table_sink_replica | int     | 否    | 1             |
| common-options           |         | 否    | -             |

### email_from_address [string]

发件人邮箱地址

### email_to_address [string]

接收邮件的地址，支持多个邮箱地址，以逗号（,）分隔。

示例：`receiver-1@example.com,receiver-2@example.com`。

### email_host [string]

连接的SMTP服务器地址

### email_transport_protocol [string]

发送邮件使用的传输协议，通常为 `smtp`（或 `smtps`）。

### email_smtp_auth [boolean]

是否启用 SMTP 认证。设为 `true` 时，连接器会自动开启 SSL，并以 `email_from_address` 作为用户名、`email_authorization_code` 作为密码进行认证。设为 `false` 时，连接器通过普通 SMTP 发送邮件，不进行认证。

### email_smtp_port [int]

SMTP 服务器端口，取值必须在 `1` 到 `65535` 之间（包含边界值）。默认值 `465` 为 SMTPS 端口，需与 `email_smtp_auth = true` 配合使用。如果使用不带认证的普通 SMTP，请填写与服务匹配的端口（例如 `25` 或 `3025`）。

### email_authorization_code [string]

当 `email_smtp_auth = true` 时，用于与 `email_from_address` 一起对 SMTP 会话进行认证的授权码或密码。可以从邮箱设置中获取（例如 QQ 邮箱、Gmail、163 邮箱等提供的专用密码 / 授权码）。

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

### multi_table_sink_replica [int]

多表写入时，每张表使用的 Sink Writer 副本数。默认值为 `1`。

### 空数据行为

只有上游至少写入一行数据时，Email Sink 才会发送邮件。如果上游表没有数据，则不会发送邮件。

### common options

Sink插件常用参数，请参考 [Sink常用选项](../common-options/sink-common-options.md) 了解详情.

:::tip

认证与 SSL

- 当 `email_smtp_auth = true` 时，连接器会开启 SSL（`mail.smtp.ssl.enable`）并信任所有 SMTP 主机，因此默认端口 `465`（SMTPS）适用于 QQ 邮箱、Gmail、163 邮箱等服务商。请使用邮箱的授权码（专用密码）作为 `email_authorization_code`，而不是账号登录密码。
- 当 `email_smtp_auth = false` 时，连接器通过不带 SSL 的普通 SMTP 发送邮件，请选择匹配的普通 SMTP 端口（例如 `25` 或本地测试服务的 `3025`）。

:::

## 示例

### 发送单表数据到多个收件人

这个示例使用不需要认证的 SMTP 服务，并发送一封收件人列表由 `email_to_address` 决定的邮件。

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
    email_host = "smtp.example.com"
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

Email sink 支持多表输入。两个上游表会让每个收件人收到两封邮件。

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
    email_host = "smtp.example.com"
    email_transport_protocol = "smtp"
    email_smtp_auth = false
    email_smtp_port = 3025
    email_authorization_code = ""
    email_message_headline = "test-title"
    email_message_content = "test-content"
  }
}
```

### 使用 SMTP 认证发送

本示例通过 QQ 邮箱等需要认证的 SMTP 服务器发送邮件。当 `email_smtp_auth = true` 时，连接器会自动开启 SSL，并使用 `email_from_address` 和 `email_authorization_code` 进行认证。请将授权码替换为你在邮箱设置中生成的授权码。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        id = bigint
        name = string
        age = int
      }
    }
  }
}

sink {
  EmailSink {
    email_from_address = "xxxxxxxx@qq.com"
    email_to_address = "xxxxxxxx@qq.com"
    email_host = "smtp.qq.com"
    email_transport_protocol = "smtp"
    email_smtp_auth = true
    email_authorization_code = "your-authorization-code"
    email_message_headline = "test-title"
    email_message_content = "test-content"
  }
}
```

## 变更日志

<ChangeLog />
