# LLM

> LLM 转换插件

## 描述

利用大型语言模型 (LLM) 的强大功能来处理数据，方法是将数据发送到 LLM 并接收生成的结果。利用 LLM 的功能来标记、清理、丰富数据、执行数据推理等。

## 属性

| 名称                     | 类型   | 是否必须 | 默认值         |
|------------------------| ------ | -------- |-------------|
| model_provider         | enum   | yes      |             |
| output_data_type       | enum   | no       | String      |
| output_column_name     | string | no       | llm_output   |
| prompt                 | string | yes      |             |
| inference_columns      | list   | no       |             |
| model                  | string | yes      |             |
| api_key                | string | yes      |             |
| api_path               | string | no       |             |
| custom_config          | map    | no       |             |
| custom_response_parse  | string | no       |             |
| custom_request_headers | map    | no       |             |
| custom_request_body    | map    | no       |             |

### model_provider

要使用的模型提供者。可用选项为:
OPENAI,DOUBAO,DEEPSEEK,KIMIAI,MICROSOFT, CUSTOM

> tips: 如果使用 Microsoft, 请确保 api_path 配置不能为空

### output_data_type

输出数据的数据类型。可用选项为:
STRING,INT,BIGINT,DOUBLE,BOOLEAN.
默认值为 STRING。

### output_column_name

自定义输出数据字段名称。自定义字段名称与现有字段名称相同时,将替换为`llm_output`。

### prompt

发送到 LLM 的提示。此参数定义 LLM 将如何处理和返回数据，例如:

从源读取的数据是这样的表格:

| name          | age |
|---------------|-----|
| Jia Fan       | 20  |
| Hailin Wang   | 20  |
| Eric          | 20  |
| Guangdong Liu | 20  |

我们可以使用以下提示:

```
Determine whether someone is Chinese or American by their name
```

这将返回:

| name          | age | llm_output |
|---------------|-----|------------|
| Jia Fan       | 20  | Chinese    |
| Hailin Wang   | 20  | Chinese    |
| Eric          | 20  | American   |
| Guangdong Liu | 20  | Chinese    |

### inference_columns

`inference_columns`选项允许您指定应该将输入数据中的哪些列用作LLM的输入。默认情况下，所有列都将用作输入。

For example:
```hocon
transform {
  LLM {
    model_provider = OPENAI
    model = gpt-4o-mini
    api_key = sk-xxx
    inference_columns = ["name", "age"]
    prompt = "Determine whether someone is Chinese or American by their name"
  }
}
```

### model

要使用的模型。不同的模型提供者有不同的模型。例如，OpenAI 模型可以是 `gpt-4o-mini`。
如果使用 OpenAI 模型，请参考 https://platform.openai.com/docs/models/model-endpoint-compatibility 文档的`/v1/chat/completions` 端点。

### api_key

用于模型提供者的 API 密钥。
如果使用 OpenAI 模型，请参考 https://platform.openai.com/docs/api-reference/api-keys 文档的如何获取 API 密钥。

### api_path

用于模型提供者的 API 路径。在大多数情况下，您不需要更改此配置。如果使用 API 代理的服务，您可能需要将其配置为代理的 API 地址。

### custom_config

`custom_config` 选项允许您为模型提供额外的自定义配置。这是一个 Map，您可以在其中定义特定模型可能需要的各种设置。

### custom_response_parse

`custom_response_parse` 选项允许您指定如何解析模型的响应。您可以使用 JsonPath
从响应中提取所需的特定数据。例如，使用 `$.choices[*].message.content` 提取如下json中的 `content` 字段
值。JsonPath 的使用请参考 [JsonPath 快速入门](https://github.com/json-path/JsonPath?tab=readme-ov-file#getting-started)

```json
{
  "id": "chatcmpl-9s4hoBNGV0d9Mudkhvgzg64DAWPnx",
  "object": "chat.completion",
  "created": 1722674828,
  "model": "gpt-4o-mini",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "[\"Chinese\"]"
      },
      "logprobs": null,
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 107,
    "completion_tokens": 3,
    "total_tokens": 110
  },
  "system_fingerprint": "fp_0f03d4f0ee",
  "code": 0,
  "msg": "ok"
}
```

### custom_request_headers

`custom_request_headers` 选项允许您定义应包含在发送到模型 API 的请求中的自定义头信息。如果 API
需要标准头信息之外的额外头信息，例如授权令牌、内容类型等，这个选项会非常有用。

### custom_request_body

`custom_request_body` 选项支持占位符：

- `${model}`：用于模型名称的占位符。
- `${input}`：用于确定输入值的占位符,同时根据 body value 的类型定义请求体请求类型。例如：`"${input}"` -> "input"。
- `${prompt}`：用于 LLM 模型提示的占位符。

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## tips
大模型API接口通常会有速率限制，可以配合Seatunnel的限速配置，已确保任务顺利运行。
Seatunnel限速配置,请参考[speed-limit](../concept/speed-limit.md)了解详情

## 示例 OPENAI

通过 LLM 确定用户所在的国家。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  read_limit.rows_per_second = 10
}

source {
  FakeSource {
    row.num = 5
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
    rows = [
      {fields = [1, "Jia Fan"], kind = INSERT}
      {fields = [2, "Hailin Wang"], kind = INSERT}
      {fields = [3, "Tomas"], kind = INSERT}
      {fields = [4, "Eric"], kind = INSERT}
      {fields = [5, "Guangdong Liu"], kind = INSERT}
    ]
  }
}

transform {
  LLM {
    model_provider = OPENAI
    model = gpt-4o-mini
    api_key = sk-xxx
    prompt = "Determine whether someone is Chinese or American by their name"
  }
}

sink {
  console {
  }
}
```

## 示例 KIMIAI

通过 LLM 判断人名是否中国历史上的帝王

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  read_limit.rows_per_second = 10
}

source {
  FakeSource {
    row.num = 5
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
    rows = [
      {fields = [1, "诸葛亮"], kind = INSERT}
      {fields = [2, "李世民"], kind = INSERT}
      {fields = [3, "孙悟空"], kind = INSERT}
      {fields = [4, "朱元璋"], kind = INSERT}
      {fields = [5, "乔治·华盛顿"], kind = INSERT}
    ]
  }
}

transform {
  LLM {
    model_provider = KIMIAI
    model = moonshot-v1-8k
    api_key = sk-xxx
    prompt = "判断是否是中国历史上的帝王"
    output_data_type = boolean
  }
}

sink {
  console {
  }
}
```
### Customize the LLM model

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 5
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
    rows = [
      {fields = [1, "Jia Fan"], kind = INSERT}
      {fields = [2, "Hailin Wang"], kind = INSERT}
      {fields = [3, "Tomas"], kind = INSERT}
      {fields = [4, "Eric"], kind = INSERT}
      {fields = [5, "Guangdong Liu"], kind = INSERT}
    ]
    plugin_output = "fake"
  }
}

transform {
  LLM {
    plugin_input = "fake"
    model_provider = CUSTOM
    model = gpt-4o-mini
    api_key = sk-xxx
    prompt = "Determine whether someone is Chinese or American by their name"
    openai.api_path = "http://mockserver:1080/v1/chat/completions"
    custom_config={
            custom_response_parse = "$.choices[*].message.content"
            custom_request_headers = {
                Content-Type = "application/json"
                Authorization = "Bearer xxxxxxxx"            
            }
            custom_request_body ={
                model = "${model}"
                messages = [
                {
                    role = "system"
                    content = "${prompt}"
                },
                {
                    role = "user"
                    content = "${input}"
                }]
            }
        }
    plugin_output = "llm_output"
  }
}

sink {
  Assert {
    plugin_input = "llm_output"
    rules =
      {
        field_rules = [
          {
            field_name = llm_output
            field_type = string
            field_value = [
              {
                rule_type = NOT_NULL
              }
            ]
          }
        ]
      }
  }
}
```
