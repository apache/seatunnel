# Feishu Sheet

> Feishu sheet source connector

## Description

Get data from Feishu sheet

:::tip

Engine Supported and plugin name

* [x] Spark: FeishuSheet
* [ ] Flink

:::

## Options

| name           | type   | required | default value       |
| ---------------| ------ |----------|---------------------|
| appId          | string | yes      | -                   |
| appSecret      | string | yes      | -                   |
| sheetToken     | string | yes      | -                   |
| range          | string | no       | all values in sheet |
| sheetNum       | int    | no       | 1                   |
| titleLineNum   | int    | no       | 1                   |
| ignoreTitleLine| bool   | no       | true                |

* appId and appSecret
  * These two parameters need to get from Feishu open platform.
  * And open the sheet permission in permission management tab.
* sheetToken
  * If you Feishu sheet link is https://xxx.feishu.cn/sheets/shtcnGxninxxxxxxx
  and the "shtcnGxninxxxxxxx" is sheetToken.
* range 
  * The format is A1:D5 or A2:C4 and so on.
* sheetNum
  * If you want import first sheet you can input 1 and the default value is 1.
  * If you want import second one you should input 2.
* titleLineNum
  * The default title line the first line.
  * If you title line is not first, you can change number for it. Like 2, 3 or 5.
* ignoreTitleLine
  * The title line it not save to data, if you want to save title line to data, you can set value as false.

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details

## Example

```bash
    FeishuSheet {
        result_table_name = "my_dataset"
        appId = "cli_a2cbxxxxxx"
        appSecret = "IvhtW7xxxxxxxxxxxxxxx"
        sheetToken = "shtcn6K3DIixxxxxxxxxxxx"
        # range = "A1:D4"
    }
```