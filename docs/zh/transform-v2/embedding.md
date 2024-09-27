# Embedding

> Embedding Transform Plugin

## 描述

`Embedding` 转换插件利用 embedding 模型将文本数据转换为向量化表示。此转换可以应用于各种字段。该插件支持多种模型提供商，并且可以与不同的API集成。

## 配置选项

| 名称                             | 类型     | 是否必填 | 默认值 | 描述                                                               |
|--------------------------------|--------|------|-----|------------------------------------------------------------------|
| model_provider                 | enum   | 是    | -   | embedding模型的提供商。可选项包括 `QIANFAN`、`OPENAI` 等。                      |
| api_key                        | string | 是    | -   | 用于验证embedding服务的API密钥。                                           |
| secret_key                     | string | 是    | -   | 用于额外验证的密钥。一些提供商可能需要此密钥进行安全的API请求。                                |
| single_vectorized_input_number | int    | 否    | 1   | 单次请求向量化的输入数量。默认值为1。                                              |
| vectorization_fields           | map    | 是    | -   | 输入字段和相应的输出向量字段之间的映射。                                             |
| model                          | string | 是    | -   | 要使用的具体embedding模型。例如，如果提供商为OPENAI，可以指定 `text-embedding-3-small`。 |
| api_path                       | string | 否    | -   | embedding服务的API。通常由模型提供商提供。                                      |
| oauth_path                     | string | 否    | -   | oauth 服务的 API 。                                                  |
| custom_config                  | map    | 否    |     | 模型的自定义配置。                                                        |
| custom_response_parse          | string | 否    |     | 使用 JsonPath 解析模型响应的方式。示例：`$.choices[*].message.content`。         |
| custom_request_headers         | map    | 否    |     | 发送到模型的请求的自定义头信息。                                                 |
| custom_request_body            | map    | 否    |     | 请求体的自定义配置。支持占位符如 `${model}`、`${input}`。                          |

### embedding_model_provider

用于生成 embedding 的模型提供商。常见选项包括 `DOUBAO`、`QIANFAN`、`OPENAI` 等，同时可选择 `CUSTOM` 实现自定义 embedding
模型的请求以及获取。

### api_key

用于验证 embedding 服务请求的API密钥。通常由模型提供商在你注册他们的服务时提供。

### secret_key

用于额外验证的密钥。一些提供商可能要求此密钥以确保API请求的安全性。

### single_vectorized_input_number

指定单次请求向量化的输入数量。默认值为1。根据处理能力和模型提供商的API限制进行调整。

### vectorization_fields

输入字段和相应的输出向量字段之间的映射。这使得插件可以理解要向量化的文本字段以及如何存储生成的向量。

```hocon
vectorization_fields {
    book_intro_vector = book_intro
    author_biography_vector  = author_biography
}
```

### model

要使用的具体 embedding 模型。这取决于`embedding_model_provider`。例如，如果使用 OPENAI ，可以指定 `text-embedding-3-small`。

### api_path

用于向 embedding 服务发送请求的API。根据提供商和所用模型的不同可能有所变化。通常由模型提供商提供。

### oauth_path

用于向oauth服务发送请求的API,获取对应的认证信息。根据提供商和所用模型的不同可能有所变化。通常由模型提供商提供。

### custom_config

`custom_config` 选项允许您为模型提供额外的自定义配置。这是一个映射，您可以在其中定义特定模型可能需要的各种设置。

### custom_response_parse

`custom_response_parse` 选项允许您指定如何解析模型的响应。您可以使用 JsonPath
从响应中提取所需的特定数据。例如，使用 `$.data[*].embedding` 提取如下json中的 `embedding` 字段
值,获取 `List` 嵌套 `List` 的结果。JsonPath
的使用请参考 [JsonPath 快速入门](https://github.com/json-path/JsonPath?tab=readme-ov-file#getting-started)

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "index": 0,
      "embedding": [
        -0.006929283495992422,
        -0.005336422007530928,
        -0.00004547132266452536,
        -0.024047505110502243
      ]
    }
  ],
  "model": "text-embedding-3-small",
  "usage": {
    "prompt_tokens": 5,
    "total_tokens": 5
  }
}
```

### custom_request_headers

`custom_request_headers` 选项允许您定义应包含在发送到模型 API 的请求中的自定义头信息。如果 API
需要标准头信息之外的额外头信息，例如授权令牌、内容类型等，这个选项会非常有用。

### custom_request_body

`custom_request_body` 选项支持占位符：

- `${model}`：用于模型名称的占位符。
- `${input}`：用于确定输入值的占位符,同时根据 body value 的类型定义请求体请求类型。例如：`["${input}"]` -> ["input"] (
  list)。

### common options

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## 示例配置

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 5
    schema = {
      fields {
        book_id = "int"
        book_name = "string"
        book_intro = "string"
        author_biography = "string"
      }
    }
    rows = [
      {fields = [1, "To Kill a Mockingbird",
      "Set in the American South during the 1930s, To Kill a Mockingbird tells the story of young Scout Finch and her brother, Jem, who are growing up in a world of racial inequality and injustice. Their father, Atticus Finch, is a lawyer who defends a black man falsely accused of raping a white woman, teaching his children valuable lessons about morality, courage, and empathy.",
      "Harper Lee (1926–2016) was an American novelist best known for To Kill a Mockingbird, which won the Pulitzer Prize in 1961. Lee was born in Monroeville, Alabama, and the town served as inspiration for the fictional Maycomb in her novel. Despite the success of her book, Lee remained a private person and published only one other novel, Go Set a Watchman, which was written before To Kill a Mockingbird but released in 2015 as a sequel."
      ], kind = INSERT}
      {fields = [2, "1984",
      "1984 is a dystopian novel set in a totalitarian society governed by Big Brother. The story follows Winston Smith, a man who works for the Party rewriting history. Winston begins to question the Party’s control and seeks truth and freedom in a society where individuality is crushed. The novel explores themes of surveillance, propaganda, and the loss of personal autonomy.",
      "George Orwell (1903–1950) was the pen name of Eric Arthur Blair, an English novelist, essayist, journalist, and critic. Orwell is best known for his works 1984 and Animal Farm, both of which are critiques of totalitarian regimes. His writing is characterized by lucid prose, awareness of social injustice, opposition to totalitarianism, and support of democratic socialism. Orwell’s work remains influential, and his ideas have shaped contemporary discussions on politics and society."
      ], kind = INSERT}
      {fields = [3, "Pride and Prejudice",
      "Pride and Prejudice is a romantic novel that explores the complex relationships between different social classes in early 19th century England. The story centers on Elizabeth Bennet, a young woman with strong opinions, and Mr. Darcy, a wealthy but reserved gentleman. The novel deals with themes of love, marriage, and societal expectations, offering keen insights into human behavior.",
      "Jane Austen (1775–1817) was an English novelist known for her sharp social commentary and keen observations of the British landed gentry. Her works, including Sense and Sensibility, Emma, and Pride and Prejudice, are celebrated for their wit, realism, and biting critique of the social class structure of her time. Despite her relatively modest life, Austen’s novels have gained immense popularity, and she is considered one of the greatest novelists in the English language."
      ], kind = INSERT}
      {fields = [4, "The Great GatsbyThe Great Gatsby",
      "The Great Gatsby is a novel about the American Dream and the disillusionment that can come with it. Set in the 1920s, the story follows Nick Carraway as he becomes entangled in the lives of his mysterious neighbor, Jay Gatsby, and the wealthy elite of Long Island. Gatsby's obsession with the beautiful Daisy Buchanan drives the narrative, exploring themes of wealth, love, and the decay of the American Dream.",
      "F. Scott Fitzgerald (1896–1940) was an American novelist and short story writer, widely regarded as one of the greatest American writers of the 20th century. Born in St. Paul, Minnesota, Fitzgerald is best known for his novel The Great Gatsby, which is often considered the quintessential work of the Jazz Age. His works often explore themes of youth, wealth, and the American Dream, reflecting the turbulence and excesses of the 1920s."
      ], kind = INSERT}
      {fields = [5, "Moby-Dick",
      "Moby-Dick is an epic tale of obsession and revenge. The novel follows the journey of Captain Ahab, who is on a relentless quest to kill the white whale, Moby Dick, that once maimed him. Narrated by Ishmael, a sailor aboard Ahab’s ship, the story delves into themes of fate, humanity, and the struggle between man and nature. The novel is also rich with symbolism and philosophical musings.",
      "Herman Melville (1819–1891) was an American novelist, short story writer, and poet of the American Renaissance period. Born in New York City, Melville gained initial fame with novels such as Typee and Omoo, but it was Moby-Dick, published in 1851, that would later be recognized as his masterpiece. Melville’s work is known for its complexity, symbolism, and exploration of themes such as man’s place in the universe, the nature of evil, and the quest for meaning. Despite facing financial difficulties and critical neglect during his lifetime, Melville’s reputation soared posthumously, and he is now considered one of the great American authors."
      ], kind = INSERT}
    ]
    result_table_name = "fake"
  }
}

transform {
  Embedding {
    source_table_name = "fake"
    embedding_model_provider = QIANFAN
    model = bge_large_en
    api_key = xxxxxxxxxx
    secret_key = xxxxxxxxxx
    api_path = xxxxxxxxxx
    vectorization_fields {
        book_intro_vector = book_intro
        author_biography_vector  = author_biography
    }
    result_table_name = "embedding_output"
  }
}

sink {
  Assert {
      source_table_name = "embedding_output"


      rules =
        {
          field_rules = [
            {
              field_name = book_id
              field_type = int
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = book_name
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = book_intro
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = author_biography
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = book_intro_vector
              field_type = float_vector
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = author_biography_vector
              field_type = float_vector
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

### Customize the embedding model

```hocon

env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 5
    schema = {
      fields {
        book_id = "int"
        book_name = "string"
        book_intro = "string"
        author_biography = "string"
      }
    }
    rows = [
      {fields = [1, "To Kill a Mockingbird",
      "Set in the American South during the 1930s, To Kill a Mockingbird tells the story of young Scout Finch and her brother, Jem, who are growing up in a world of racial inequality and injustice. Their father, Atticus Finch, is a lawyer who defends a black man falsely accused of raping a white woman, teaching his children valuable lessons about morality, courage, and empathy.",
      "Harper Lee (1926–2016) was an American novelist best known for To Kill a Mockingbird, which won the Pulitzer Prize in 1961. Lee was born in Monroeville, Alabama, and the town served as inspiration for the fictional Maycomb in her novel. Despite the success of her book, Lee remained a private person and published only one other novel, Go Set a Watchman, which was written before To Kill a Mockingbird but released in 2015 as a sequel."
      ], kind = INSERT}
      {fields = [2, "1984",
      "1984 is a dystopian novel set in a totalitarian society governed by Big Brother. The story follows Winston Smith, a man who works for the Party rewriting history. Winston begins to question the Party’s control and seeks truth and freedom in a society where individuality is crushed. The novel explores themes of surveillance, propaganda, and the loss of personal autonomy.",
      "George Orwell (1903–1950) was the pen name of Eric Arthur Blair, an English novelist, essayist, journalist, and critic. Orwell is best known for his works 1984 and Animal Farm, both of which are critiques of totalitarian regimes. His writing is characterized by lucid prose, awareness of social injustice, opposition to totalitarianism, and support of democratic socialism. Orwell’s work remains influential, and his ideas have shaped contemporary discussions on politics and society."
      ], kind = INSERT}
      {fields = [3, "Pride and Prejudice",
      "Pride and Prejudice is a romantic novel that explores the complex relationships between different social classes in early 19th century England. The story centers on Elizabeth Bennet, a young woman with strong opinions, and Mr. Darcy, a wealthy but reserved gentleman. The novel deals with themes of love, marriage, and societal expectations, offering keen insights into human behavior.",
      "Jane Austen (1775–1817) was an English novelist known for her sharp social commentary and keen observations of the British landed gentry. Her works, including Sense and Sensibility, Emma, and Pride and Prejudice, are celebrated for their wit, realism, and biting critique of the social class structure of her time. Despite her relatively modest life, Austen’s novels have gained immense popularity, and she is considered one of the greatest novelists in the English language."
      ], kind = INSERT}
      {fields = [4, "The Great GatsbyThe Great Gatsby",
      "The Great Gatsby is a novel about the American Dream and the disillusionment that can come with it. Set in the 1920s, the story follows Nick Carraway as he becomes entangled in the lives of his mysterious neighbor, Jay Gatsby, and the wealthy elite of Long Island. Gatsby's obsession with the beautiful Daisy Buchanan drives the narrative, exploring themes of wealth, love, and the decay of the American Dream.",
      "F. Scott Fitzgerald (1896–1940) was an American novelist and short story writer, widely regarded as one of the greatest American writers of the 20th century. Born in St. Paul, Minnesota, Fitzgerald is best known for his novel The Great Gatsby, which is often considered the quintessential work of the Jazz Age. His works often explore themes of youth, wealth, and the American Dream, reflecting the turbulence and excesses of the 1920s."
      ], kind = INSERT}
      {fields = [5, "Moby-Dick",
      "Moby-Dick is an epic tale of obsession and revenge. The novel follows the journey of Captain Ahab, who is on a relentless quest to kill the white whale, Moby Dick, that once maimed him. Narrated by Ishmael, a sailor aboard Ahab’s ship, the story delves into themes of fate, humanity, and the struggle between man and nature. The novel is also rich with symbolism and philosophical musings.",
      "Herman Melville (1819–1891) was an American novelist, short story writer, and poet of the American Renaissance period. Born in New York City, Melville gained initial fame with novels such as Typee and Omoo, but it was Moby-Dick, published in 1851, that would later be recognized as his masterpiece. Melville’s work is known for its complexity, symbolism, and exploration of themes such as man’s place in the universe, the nature of evil, and the quest for meaning. Despite facing financial difficulties and critical neglect during his lifetime, Melville’s reputation soared posthumously, and he is now considered one of the great American authors."
      ], kind = INSERT}
    ]
    result_table_name = "fake"
  }
}

transform {
 Embedding {
    source_table_name = "fake"
    model_provider = CUSTOM
    model = text-embedding-3-small
    api_key = xxxxxxxx
    api_path = "http://mockserver:1080/v1/doubao/embedding"
    single_vectorized_input_number = 2
    vectorization_fields {
        book_intro_vector = book_intro
        author_biography_vector  = author_biography
    }
    custom_config={
        custom_response_parse = "$.data[*].embedding"
        custom_request_headers = {
            "Content-Type"= "application/json"
            "Authorization"= "Bearer xxxxxxx
        }
        custom_request_body ={
            modelx = "${model}"
            inputx = ["${input}"]
        }
    }
    result_table_name = "embedding_output_1"
  }
}

sink {
  Assert {
      source_table_name = "embedding_output_1"
      rules =
        {
          field_rules = [
            {
              field_name = book_id
              field_type = int
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = book_name
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = book_intro
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = author_biography
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = book_intro_vector
              field_type = float_vector
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            },
            {
              field_name = author_biography_vector
              field_type = float_vector
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