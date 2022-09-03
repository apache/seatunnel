# Datahub

> Datahub sink connector

## Description

A sink plugin which use send message to datahub

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name       | type   | required | default value |
|------------|--------|----------|---------------|
| endpoint   | string | yes      | -             |
| accessId   | string | yes      | -             |
| accessKey  | string | yes      | -             |
| project    | string | yes      | -             |
| topic      | string | yes      | -             |
| timeout    | int    | yes      | -             |
| retryTimes | int    | yes      | -             |

### url [string]

your datahub endpoint start with http （string）

### accessId [string]

your datahub accessId which cloud be access from Alibaba Cloud  (string)

### accessKey[string]

your datahub accessKey which cloud be access from Alibaba Cloud  (string)

### project [string]

your datahub project which is created in Alibaba Cloud  (string)

### topic [string]

your datahub topic  (string)

### timeout [int]

the max connection timeout (int)

### retryTimes [int]

the max retry times when your client put record failed  (int)

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
