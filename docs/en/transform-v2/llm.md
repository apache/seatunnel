# LLM

> LLM transform plugin

## Description

Leverage the power of a large language model (LLM) to process data by sending it to the LLM and receiving the
generated results. Utilize the LLM's capabilities to label, clean, enrich data, perform data inference, and
more.

## Options

| name                   | type   | required | default value |
|------------------------|--------|----------|---------------|
| model_provider         | enum   | yes      |               |
| output_data_type       | enum   | no       | String        |
| output_column_name     | string | no       | llm_output    |
| prompt                 | string | yes      |               |
| inference_columns      | list   | no       |               |
| model                  | string | yes      |               |
| api_key                | string | yes      |               |
| api_path               | string | no       |               |
| custom_config          | map    | no       |               |
| custom_response_parse  | string | no       |               |
| custom_request_headers | map    | no       |               |
| custom_request_body    | map    | no       |               |

### model_provider

The model provider to use. The available options are:
OPENAI, DOUBAO, DEEPSEEK, KIMIAI, MICROSOFT, CUSTOM

> tips: If you use Microsoft, please make sure api_path cannot be empty

### output_data_type

The data type of the output data. The available options are:
STRING,INT,BIGINT,DOUBLE,BOOLEAN.
Default value is STRING.

### output_column_name

Custom output data field name. A custom field name that is the same as an existing field name is replaced with 'llm_output'.

### prompt

The prompt to send to the LLM. This parameter defines how LLM will process and return data, eg:

The data read from source is a table like this:

| name          | age |
|---------------|-----|
| Jia Fan       | 20  |
| Hailin Wang   | 20  |
| Eric          | 20  |
| Guangdong Liu | 20  |

The prompt can be:

```
Determine whether someone is Chinese or American by their name
```

The result will be:

| name          | age | llm_output |
|---------------|-----|------------|
| Jia Fan       | 20  | Chinese    |
| Hailin Wang   | 20  | Chinese    |
| Eric          | 20  | American   |
| Guangdong Liu | 20  | Chinese    |

### inference_columns

The `inference_columns` option allows you to specify which columns from the input data should be used as inputs for the LLM. By default, all columns will be used as inputs.

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

The model to use. Different model providers have different models. For example, the OpenAI model can be `gpt-4o-mini`.
If you use OpenAI model, please refer https://platform.openai.com/docs/models/model-endpoint-compatibility
of `/v1/chat/completions` endpoint.

### api_key

The API key to use for the model provider.
If you use OpenAI model, please refer https://platform.openai.com/docs/api-reference/api-keys of how to get the API key.

### api_path

The API path to use for the model provider. In most cases, you do not need to change this configuration. If you
are using an API agent's service, you may need to configure it to the agent's API address.

### custom_config

The `custom_config` option allows you to provide additional custom configurations for the model. This is a map where you
can define various settings that might be required by the specific model you're using.

### custom_response_parse

The `custom_response_parse` option allows you to specify how to parse the model's response. You can use JsonPath to
extract the specific data you need from the response. For example, by using `$.choices[*].message.content`, you can
extract the `content` field values from the following JSON. For more details on using JsonPath, please refer to
the [JsonPath Getting Started guide](https://github.com/json-path/JsonPath?tab=readme-ov-file#getting-started).

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

The `custom_request_headers` option allows you to define custom headers that should be included in the request sent to
the model's API. This is useful if the API requires additional headers beyond the standard ones, such as authorization
tokens, content types, etc.

### custom_request_body

The `custom_request_body` option supports placeholders:

- `${model}`: Placeholder for the model name.
- `${input}`: Placeholder to determine input value and define request body request type based on the type of body
  value. Example: `"${input}"` -> "input"
- `${prompt}`：Placeholder for LLM model prompts.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## tips
The API interface usually has a rate limit, which can be configured with Seatunnel's speed limit to ensure smooth operation of the task.
For details about Seatunnel speed limit Settings, please refer to [speed-limit](../concept/speed-limit.md) for details.

## Example OPENAI

Determine the user's country through a LLM.

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

## Example KIMIAI

Determine whether a person is a historical emperor of China.

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
      {fields = [1, "Zhuge Liang"], kind = INSERT}
      {fields = [2, "Li Shimin"], kind = INSERT}
      {fields = [3, "Sun Wukong"], kind = INSERT}
      {fields = [4, "Zhu Yuanzhuang"], kind = INSERT}
      {fields = [5, "George Washington"], kind = INSERT}
    ]
  }
}

transform {
  LLM {
    model_provider = KIMIAI
    model = moonshot-v1-8k
    api_key = sk-xxx
    prompt = "Determine whether a person is a historical emperor of China"
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
