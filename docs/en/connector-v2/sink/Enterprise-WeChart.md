# Enterprise WeChart

> Enterprise WeChart sink connector

## Description

A sink plugin which use Enterprise WeChart robot send message

##  Options

| name | type   | required | default value |
| --- |--------| --- | --- |
| url | String | Yes | - |
### url [string]

Enterprise WeChart webhook url format is https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX（string）

## Example

simple:

```hocon
WeChart {
        url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=693axxx6-7aoc-4bc4-97a0-0ec2sifa5aaa"
    }
```

