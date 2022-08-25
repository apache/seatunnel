# Datahub

> Datahub sink connector

## Description

A sink plugin which use send message to datahub

## Options

| name                    | type   | required | default value |
|-------------------------|--------|----------|---------------|
| endpoint                | string | yes      | -             |
| accessId                | string | yes      | -             |
| accessKey               | string | yes      | -             |
| project                 | string | yes      | -             |
| topic                   | string | yes      | -             |

### url [string]

datahub endpoint  format is http://dh-cn-hangzhou.aliyuncs.com（string）

### accessId [string]

your datahub accessId which cloud be access from Alibaba Cloud  (string)

### accessKey[string]

your datahub accessKey which cloud be access from Alibaba Cloud  (string)

### project [string]

your datahub project which is created in Alibaba Cloud  (string)

### topic [string]

your datahub topic  (string)

## Example

```hocon
sink {
 DataHub {
  endpoint="http://dh-cn-hangzhou.aliyuncs.com"
  accessId="xxx"
  accessKey="xxx"
  project="projectname"
  topic="topicname"
 }
}
```
