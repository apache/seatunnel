import ChangeLog from '../changelog/connector-email.md';

# Email

> Email sink connector

## Description

Send the received rows as an attachment file to one or more email addresses.

The connector buffers the rows of each table into a delimited attachment file (one row per line, no
header row) and sends one email per table when the writer closes. If a table has no rows, no email is
sent for it.

The tested email version is 1.5.6.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

|           name           |  type   | required | default value |
|--------------------------|---------|----------|---------------|
| email_from_address       | string  | yes      | -             |
| email_to_address         | string  | yes      | -             |
| email_host               | string  | yes      | -             |
| email_transport_protocol | string  | yes      | -             |
| email_smtp_auth          | boolean | yes      | -             |
| email_smtp_port          | int     | no       | 465           |
| email_authorization_code | string  | yes      | -             |
| email_message_headline   | string  | yes      | -             |
| email_message_content    | string  | yes      | -             |
| email_attachment_name    | string  | no       | emailsink.csv |
| email_field_delimiter    | string  | no       | ,             |
| multi_table_sink_replica | int     | no       | 1             |
| common-options           |         | no       | -             |

### email_from_address [string]

Sender Email Address.

### email_to_address [string]

Address to receive mail, Support multiple email addresses, separated by commas (,).

Example: `receiver-1@example.com,receiver-2@example.com`.

### email_host [string]

SMTP server to connect to.

### email_transport_protocol [string]

The transport protocol used to send the message, typically `smtp` (or `smtps`).

### email_smtp_auth [boolean]

Whether to use SMTP authentication. When set to `true`, the connector enables SSL automatically and
authenticates with `email_from_address` as the username and `email_authorization_code` as the
password. When set to `false`, the connector sends mail over plain SMTP without authentication.

### email_smtp_port [int]

SMTP server port. The value must be between `1` and `65535`, inclusive. The default `465` is the SMTPS port and is used together with
`email_smtp_auth = true`. For plain SMTP without authentication, set the port that matches the
server (for example `25` or `3025`).

### email_authorization_code [string]

Authorization code or password used to authenticate the SMTP session together with
`email_from_address` when `email_smtp_auth = true`. You can obtain the authorization code from the
mailbox settings (for example, the app-specific password / authorization code provided by QQ Mail,
Gmail, or 163 Mail).

This option is required by the connector configuration. When `email_smtp_auth = false`, it can be
set to an empty string.

### email_message_headline [string]

The subject line of the entire message.

### email_message_content [string]

The body of the entire message.

### email_attachment_name [string]

The name of the email attachment file. Default is `emailsink.csv`. The connector writes the rows to
this local file before sending the email.

### email_field_delimiter [string]

The delimiter used to separate fields in the attachment file. Default is comma `,`.

The attachment has no header row. Field values are written in the upstream schema order. `null`
values are written as empty strings.

### multi_table_sink_replica [int]

The replica number of sink writers used for each table in a multi-table sink job. The default value
is `1`.

### Empty input behavior

The sink sends an email only after at least one row is written. If the upstream table has no rows,
the email is skipped.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

:::tip

Authentication and SSL

- When `email_smtp_auth = true`, the connector turns on SSL (`mail.smtp.ssl.enable`) and trusts all
  SMTP hosts, so the default port `465` (SMTPS) works for providers such as QQ Mail, Gmail, and 163
  Mail. Use the mailbox's authorization code (app-specific password) as `email_authorization_code`,
  not the account login password.
- When `email_smtp_auth = false`, the connector sends over plain SMTP without SSL; choose a matching
  plain-SMTP port (for example `25` or `3025` for a local test server).

:::

## Example

### Send one table to multiple recipients

This example uses an SMTP server without authentication and sends one email whose recipient list is
defined by `email_to_address`.

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

### Send multiple tables

Email sink supports multi-table input. Two upstream tables create two emails for each recipient.

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

### Send with SMTP authentication

This example sends mail through an authenticated SMTP server such as QQ Mail. With
`email_smtp_auth = true`, the connector enables SSL automatically and uses `email_from_address`
together with `email_authorization_code` to authenticate. Replace the authorization code with the
one generated in your mailbox settings.

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

## Changelog

<ChangeLog />
