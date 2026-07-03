import ChangeLog from '../changelog/connector-email.md';

# Email

> Email sink connector

## Description

Send the received rows as an attachment file to one or more email addresses.

The tested email version is 1.5.6.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

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
| common-options           |         | no       | -             |

### email_from_address [string]

Sender Email Address.

### email_to_address [string]

Address to receive mail, Support multiple email addresses, separated by commas (,).

Example: `receiver-1@example.com,receiver-2@example.com`.

### email_host [string]

SMTP server to connect to.

### email_transport_protocol [string]

The protocol to load the session .

### email_smtp_auth [boolean]

Whether to authenticate the customer.

### email_smtp_port [int]

Select port for authentication.

### email_authorization_code [string]

Authorization code or password. You can obtain the authorization code from the mailbox settings.

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

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

### Send one table to multiple recipients

This example follows the Email e2e job. It uses a test SMTP server without authentication and sends
one email to each address in `email_to_address`.

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

### Send multiple tables

Email sink supports multi-table input. In the e2e job, two upstream tables create two emails for
each recipient.

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

## Changelog

<ChangeLog />
