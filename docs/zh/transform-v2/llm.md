# LLM

> LLM 转换插件

## 描述

利用大型语言模型 (LLM) 的强大功能来处理数据，方法是将数据发送到 LLM 并接收生成的结果。利用 LLM 的功能来标记、清理、丰富数据、执行数据推理等。

## 属性

|        名称        |   类型   | 是否必须 |                    默认值                     |
|------------------|--------|------|--------------------------------------------|
| model_provider   | enum   | yes  |                                            |
| output_data_type | enum   | no   | String                                     |
| prompt           | string | yes  |                                            |
| model            | string | yes  |                                            |
| api_key          | string | yes  |                                            |
| openai.api_path  | string | no   | https://api.openai.com/v1/chat/completions |

### model_provider

要使用的模型提供者。可用选项为:
OPENAI

### output_data_type

输出数据的数据类型。可用选项为:
STRING,INT,BIGINT,DOUBLE,BOOLEAN.
默认值为 STRING。

### prompt

发送到 LLM 的提示。此参数定义 LLM 将如何处理和返回数据，例如:

从源读取的数据是这样的表格:

|     name      | age |
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

|     name      | age | llm_output |
|---------------|-----|------------|
| Jia Fan       | 20  | Chinese    |
| Hailin Wang   | 20  | Chinese    |
| Eric          | 20  | American   |
| Guangdong Liu | 20  | Chinese    |

### model

要使用的模型。不同的模型提供者有不同的模型。例如，OpenAI 模型可以是 `gpt-4o-mini`。
如果使用 OpenAI 模型，请参考 https://platform.openai.com/docs/models/model-endpoint-compatibility 文档的`/v1/chat/completions` 端点。

### api_key

用于模型提供者的 API 密钥。
如果使用 OpenAI 模型，请参考 https://platform.openai.com/docs/api-reference/api-keys 文档的如何获取 API 密钥。

### openai.api_path

用于 OpenAI 模型提供者的 API 路径。在大多数情况下，您不需要更改此配置。如果使用 API 代理的服务，您可能需要将其配置为代理的 API 地址。

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## 示例

通过 LLM 确定用户所在的国家。

```hocon
env {
  parallelism = 1
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

