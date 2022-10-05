# DingTalk

> DinkTalk source connector
## Description

A source plugin which use DingTalk

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name          | type        | required   | default value |
|---------------| ----------  |------------| ------------- |
| api_client    | string      | yes        | -             |
| access_token  | string      | yes        | -             |
| app_key       | string      | yes        | -             |
| app_secret    | string      | yes        | -             |


### url [string]

[DingTalk API](https://open.dingtalk.com/document/orgapp-server/api-overview) address like : https://oapi.dingtalk.com/topapi/v2/department/listsub（string）

### access_token [string]

DingTalk access_token [DingTalk Doc](https://open.dingtalk.com/document/orgapp-server/obtain-the-access_token-of-an-internal-app)
The valid period of access token is 7200 seconds , if access token expired , can use app_key and app_secret get new message (string)
access_token is the source voucher , can direct use of access_token or use app_key and app_secret get it

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

result :
row=1 : {"auto_add_user":false,"create_dept_group":false,"dept_id":101254282,"ext":"{\"faceCount\":\"1\"}","name":"技术部","parent_id":1}
row=2 : {"auto_add_user":true,"create_dept_group":true,"dept_id":101279294,"name":"财务部","parent_id":1}
row=3 : {"auto_add_user":false,"create_dept_group":false,"dept_id":101316242,"ext":"{\"faceCount\":\"2\"}","name":"渠道部","parent_id":1}
row=4 : {"auto_add_user":false,"create_dept_group":false,"dept_id":101340237,"name":"运营部","parent_id":1}
row=5 : {"auto_add_user":false,"create_dept_group":false,"dept_id":101467231,"ext":"{\"faceCount\":\"1\"}","name":"客服部","parent_id":1}
row=6 : {"auto_add_user":true,"create_dept_group":true,"dept_id":101532253,"name":"人力部","parent_id":1}
row=7 : {"auto_add_user":true,"create_dept_group":true,"dept_id":101532254,"name":"直销部","parent_id":1}
```