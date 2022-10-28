# DataHub

> DataHub sink connector

## Description

A sink plugin which use send message to DataHub

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name       | type   | required | default value |
|--------------- |--------|----------|---------------|
| endpoint       | string | yes      | -             |
| accessId       | string | yes      | -             |
| accessKey      | string | yes      | -             |
| project        | string | yes      | -             |
| topic          | string | yes      | -             |
| timeout        | int    | yes      | -             |
| retryTimes     | int    | yes      | -             |
| common-options |        | no       | -             |

### url [string]

your DataHub endpoint start with http （string）

### accessId [string]

your DataHub accessId which cloud be access from Alibaba Cloud  (string)

### accessKey[string]

your DataHub accessKey which cloud be access from Alibaba Cloud  (string)

### project [string]

your DataHub project which is created in Alibaba Cloud  (string)

### topic [string]

your DataHub topic  (string)

### timeout [int]

the max connection timeout (int)

### retryTimes [int]

the max retry times when your client put record failed  (int)

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```hocon
sink {
 DataHub {
  endpoint="yourendpoint"
  accessId="xxx"
  accessKey="xxx"
  project="projectname"
  topic="topicname"
  timeout=3000
  retryTimes=3
 }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add DataHub Sink Connector
