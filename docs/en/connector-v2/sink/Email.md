# Email

> Email source connector

## Description

Send the data as a file to email.

 The tested email version is 1.5.6.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                     | type    | required | default value |
|--------------------------|---------|----------|---------------|
| email_from_address       | string  | yes      | -             |
| email_to_address         | string  | yes      | -             |
| email_host               | string  | yes      | -             |
| email_transport_protocol | string  | yes      | -             |
| email_smtp_auth          | string  | yes      | -             |
| email_authorization_code | string  | yes      | -             |
| email_message_headline   | string  | yes      | -             |
| email_message_content    | string  | yes      | -             |
| common-options           |         | no       | -             |

### email_from_address [string]

Sender Email Address .

### email_to_address [string]

Address to receive mail.

### email_host [string]

SMTP server to connect to.

### email_transport_protocol [string]

The protocol to load the session .

### email_smtp_auth [string]

Whether to authenticate the customer.

### email_authorization_code [string]

authorization code,You can obtain the authorization code from the mailbox Settings.

### email_message_headline [string]

The subject line of the entire message.

### email_message_content [string]

The body of the entire message.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.


## Example

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

## Changelog

### 2.2.0-beta 2022-09-26

- Add Email Sink Connector
