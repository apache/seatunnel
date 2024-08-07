# LLM

> LLM transform plugin

## Description

Leverage the power of a large language model (LLM) to process data by sending it to the LLM and receiving the
generated results. Utilize the LLM's capabilities to label, clean, enrich data, perform data inference, and
more.

## Options

|       name       |  type  | required |               default value                |
|------------------|--------|----------|--------------------------------------------|
| model_provider   | enum   | yes      |                                            |
| output_data_type | enum   | no       | String                                     |
| prompt           | string | yes      |                                            |
| model            | string | yes      |                                            |
| api_key          | string | yes      |                                            |
| openai.api_path  | string | no       | https://api.openai.com/v1/chat/completions |

### model_provider

The model provider to use. The available options are:
OPENAI

### output_data_type

The data type of the output data. The available options are:
STRING,INT,BIGINT,DOUBLE,BOOLEAN.
Default value is STRING.

### prompt

The prompt to send to the LLM. This parameter defines how LLM will process and return data, eg:

The data read from source is a table like this:

|     name      | age |
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

|     name      | age | llm_output |
|---------------|-----|------------|
| Jia Fan       | 20  | Chinese    |
| Hailin Wang   | 20  | Chinese    |
| Eric          | 20  | American   |
| Guangdong Liu | 20  | Chinese    |

### model

The model to use. Different model providers have different models. For example, the OpenAI model can be `gpt-4o-mini`.
If you use OpenAI model, please refer https://platform.openai.com/docs/models/model-endpoint-compatibility of `/v1/chat/completions` endpoint.

### api_key

The API key to use for the model provider.
If you use OpenAI model, please refer https://platform.openai.com/docs/api-reference/api-keys of how to get the API key.

### openai.api_path

The API path to use for the OpenAI model provider. In most cases, you do not need to change this configuration. If you are using an API agent's service, you may need to configure it to the agent's API address.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

Determine the user's country through a LLM.

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

