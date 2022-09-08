# DingTalk

> DinkTalk source connector

## Description

A source plugin which use DingTalk 

## Options

| name      | type        | required | default value |
|-----------| ----------  | -------- | ------------- |
| api_client       | string      | yes      | -             |
| access_token    | string      | yes       | -             |
| app_key    | string      | yes       | -             |
| app_secret    | string      | yes       | -             |


### url [string]

DingTalk API address like : https://oapi.dingtalk.com/topapi/v2/department/listsub（string）

### access_token [string]

DingTalk access token [DingTalk Doc](https://open.dingtalk.com/document/orgapp-server/obtain-the-access_token-of-an-internal-app) , the valid period of access token is 7200 seconds , if access token expired , can use app_key and app_secret get new message (string)

### app_key [string]

DingTalk app key (string)

### app_secret [string]

DingTalk app secret (string)

## Example

```hocon
source {
 DingTalk {
  api_client="https://oapi.dingtalk.com/topapi/v2/department/listsub"
  access_token="8c61c395035c37c7812b9b1b1dbecb20"
 }
}

or 

source {
 DingTalk {
  api_client="https://oapi.dingtalk.com/topapi/v2/department/listsub"
  app_key="dingpsi2nsmw2v"
  app_secret="qv5pLes-M9JvHXjaBqkPFdAyk9WKEjDjEZOpLZKHi"
 }
}
```
