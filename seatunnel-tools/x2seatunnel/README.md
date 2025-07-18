# X2SeaTunnel 配置转换工具
X2SeaTunnel 是一个用于将 DataX 等配置文件转换为 SeaTunnel 配置文件的工具，旨在帮助用户快速从其它数据集成平台迁移到 SeaTunnel。

## 🚀 快速开始

### 前置条件

- Java 8 或更高版本

### 安装

#### 从源码编译
```bash
# 进入 SeaTunnel 项目目录
cd /path/to/seatunnel

# 编译整个项目
mvn clean package -DskipTests

# 或者仅编译 x2seatunnel 模块
mvn clean package -pl seatunnel-tools/x2seatunnel -DskipTests
```

#### 使用发布包
```bash
# 下载并解压发布包
unzip x2seatunnel-*.zip
cd x2seatunnel-*/
```

### 基本用法

```bash
# 标准转换：使用默认模板系统，内置常见的Source和Sink
./bin/x2seatunnel.sh -s examples/source/datax-mysql2hdfs.json -t examples/target/mysql2hdfs-result.conf -r examples/report/mysql2hdfs-report.md

# 自定义任务，场景：MySQL → Hive（DataX 没有 HiveWriter）
# DataX 配置：MySQL → HDFS 自定义任务：转换为 MySQL → Hive
./bin/x2seatunnel.sh -s examples/source/datax-mysql2hdfs.json -t examples/target/mysql2hive-result.conf -r examples/report/mysql2hive-report.md -T templates/datax/custom/mysql-to-hive.conf

# YAML 配置方式（等效于上述命令行参数）
./bin/x2seatunnel.sh --config examples/yaml/datax-mysql2hdfs.yaml

# 批量转换模式：按目录处理
./bin/x2seatunnel.sh -d examples/source -o examples/target2 -R examples/report2

# 批量模式支持通配符过滤
./bin/x2seatunnel.sh -d examples/source -o examples/target3 -R examples/report3 --pattern "*-full.json" --verbose

# 查看帮助
./bin/x2seatunnel.sh --help
```



## 📁 目录结构

```
x2seatunnel/
├── bin/                        # 可执行文件
│   ├── x2seatunnel.sh         # 启动脚本
├── lib/                        # JAR包文件
│   └── x2seatunnel-*.jar      # 核心JAR包
├── config/                     # 配置文件
│   └── log4j2.xml             # 日志配置
├── templates/                  # 模板文件
│   ├── template-mapping.yaml  # 模板映射配置
│   ├── report-template.md     # 报告模板
│   └── datax/                 # DataX相关模板
│       ├── custom/            # 自定义模板
│       ├── env/               # 环境配置模板
│       ├── sources/           # 数据源模板
│       └── sinks/             # 数据目标模板
├── examples/                   # 示例和测试
│   ├── source/                # 示例源文件
│   ├── target/                # 生成的目标文件
│   └── report/                # 生成的报告
├── logs/                       # 日志文件
├── LICENSE                     # 许可证
└── README.md                   # 使用说明
```

## 🎯 功能特性

- ✅ **标准配置转换**: DataX → SeaTunnel 配置文件转换
- ✅ **自定义模板转换**: 支持用户自定义转换模板
- ✅ **详细转换报告**: 生成 Markdown 格式的转换报告
- ✅ **支持正则表达式变量提取**: 从配置中正则提取变量，支持自定义场景
- ✅ **批量转换模式**: 支持目录和文件通配符批量转换，自动生成报告和汇总报告

## 📖 使用说明

### 基本语法

```bash
x2seatunnel [OPTIONS]
```

### 命令行参数

| 选项     | 长选项          | 描述                                                 | 必需 |
|----------|-----------------|------------------------------------------------------|------|
| -s       | --source        | 源配置文件路径                                       | 是   |
| -t       | --target        | 目标配置文件路径                                     | 是   |
| -st      | --source-type   | 源配置类型 (datax, 默认: datax)                      | 否   |
| -T       | --template      | 自定义模板文件路径                                   | 否   |
| -r       | --report        | 转换报告文件路径                                     | 否   |
| -d       | --directory     | 批量转换源目录                                       | 否   |
| -o       | --output-dir    | 批量转换输出目录                                     | 否   |
| -p       | --pattern       | 文件通配符模式（逗号分隔，例如: *.json,*.xml）        | 否   |
| -R       | --report-dir    | 批量模式下报告输出目录，单文件报告和汇总 summary.md 将输出到该目录 | 否   |
| -v       | --version       | 显示版本信息                                         | 否   |
| -h       | --help          | 显示帮助信息                                         | 否   |
|          | --verbose       | 启用详细日志输出                                     | 否   |

```bash
# 示例：查看命令行帮助
./bin/x2seatunnel.sh --help
```

### 支持的配置类型

#### 源配置类型
- **datax**: DataX配置文件（JSON格式）- 默认类型

#### 目标配置类型
- **seatunnel**: SeaTunnel配置文件（HOCON格式）

## 🎨 模板系统

### 设计理念

X2SeaTunnel 采用基于 DSL (Domain Specific Language) 的模板系统，通过配置驱动的方式实现不同数据源和目标的快速适配。核心优势：

- **配置驱动**：所有转换逻辑都通过 YAML 配置文件定义，无需修改 Java 代码
- **易于扩展**：新增数据源类型只需添加模板文件和映射配置
- **统一语法**：使用 Jinja2 风格的模板语法，易于理解和维护
- **智能映射**：通过转换器（transformer）实现复杂的参数映射逻辑

### 模板语法

X2SeaTunnel 使用类似 Jinja2 的模板语法，支持以下特性：

#### 1. 基础变量访问
```hocon
# 访问 DataX 配置中的字段
user = "{{ datax.job.content[0].reader.parameter.username }}"
password = "{{ datax.job.content[0].reader.parameter.password }}"
```

#### 2. 过滤器支持
```hocon
# join 过滤器：数组连接
query = "SELECT {{ datax.job.content[0].reader.parameter.column | join(',') }} FROM table"

# default 过滤器：默认值
partition_column = "{{ datax.job.content[0].reader.parameter.splitPk | default('') }}"
fetch_size = {{ datax.job.content[0].reader.parameter.fetchSize | default(1024) }}

# 转换器调用：智能参数映射
driver = "{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] | jdbc_driver_mapper }}"
```

#### 3. 支持的过滤器

| 过滤器 | 语法 | 描述 | 示例 |
|--------|------|------|------|
| `join` | `{{ array \| join('分隔符') }}` | 数组连接 | `{{ columns \| join(',') }}` |
| `default` | `{{ value \| default('默认值') }}` | 默认值 | `{{ port \| default(3306) }}` |
| `upper` | `{{ value \| upper }}` | 大写转换 | `{{ name \| upper }}` |
| `lower` | `{{ value \| lower }}` | 小写转换 | `{{ name \| lower }}` |
| `自定义转换器` | `{{ value \| transformer_name }}` | 自定义映射 | `{{ url \| jdbc_driver_mapper }}` |

#### 4. 模板配置示例

```hocon
# MySQL到HDFS的转换模板
env {
  parallelism = {{ datax.job.setting.speed.channel | default(1) }}
  job.mode = "BATCH"
}

source {
  Jdbc {
    # 数据库连接配置
    url = "{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] }}"
    driver = "{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] | jdbc_driver_mapper }}"
    user = "{{ datax.job.content[0].reader.parameter.username }}"
    password = "{{ datax.job.content[0].reader.parameter.password }}"
    
    # 智能查询生成
    query = "{{ datax.job.content[0].reader.parameter.querySql[0] | default('SELECT') }} {{ datax.job.content[0].reader.parameter.column | join(',') }} FROM {{ datax.job.content[0].reader.parameter.connection[0].table[0] }} WHERE {{ datax.job.content[0].reader.parameter.where | default('1=1') }}"
    
    # 性能优化配置
    partition_column = "{{ datax.job.content[0].reader.parameter.splitPk | default('') }}"
    partition_num = {{ datax.job.setting.speed.channel | default(1) }}
    fetch_size = {{ datax.job.content[0].reader.parameter.fetchSize | default(1024) }}
    
    result_table_name = "source_table"
  }
}

sink {
  HdfsFile {
    path = "{{ datax.job.content[0].writer.parameter.path }}"
    file_format_type = "{{ datax.job.content[0].writer.parameter.fileType | default('text') }}"
    field_delimiter = "{{ datax.job.content[0].writer.parameter.fieldDelimiter | default('\t') }}"
  }
}
```

### 自定义转换器

通过 `templates/template-mapping.yaml` 配置自定义转换器：

```yaml
transformers:
  # JDBC 驱动映射
  jdbc_driver_mapper:
    mysql: "com.mysql.cj.jdbc.Driver"
    postgresql: "org.postgresql.Driver"
    oracle: "oracle.jdbc.driver.OracleDriver"
    sqlserver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  
  # 文件格式映射
  file_format_mapper:
    text: "text"
    orc: "orc"
    parquet: "parquet"
    json: "json"
```

### 扩展新数据源

添加新数据源类型只需三步：

1. **创建模板文件**：在 `templates/datax/sources/` 下创建新的模板文件
2. **配置映射关系**：在 `template-mapping.yaml` 中添加映射配置
3. **添加转换器**：如需特殊处理，添加对应的转换器配置

无需修改任何 Java 代码，即可支持新的数据源类型。



## 🌐 支持的数据源和目标

### 数据源（Sources）

| 数据源类型 | DataX Reader | 模板文件 | 支持状态 | 备注 |
|-----------|-------------|----------|----------|------|
| **MySQL** | `mysqlreader` | `mysql-source.conf` | ✅ 完全支持 | 自动驱动映射 |
| **PostgreSQL** | `postgresqlreader` | `jdbc-source.conf` | ✅ 完全支持 | 统一JDBC模板 |
| **Oracle** | `oraclereader` | `jdbc-source.conf` | ✅ 完全支持 | 统一JDBC模板 |
| **SQL Server** | `sqlserverreader` | `jdbc-source.conf` | ✅ 完全支持 | 统一JDBC模板 |
| **ClickHouse** | `clickhousereader` | `jdbc-source.conf` | 🔧 开发中 | 统一JDBC模板 |
| **Hive** | `hivereader` | `hive-source.conf` | 📋 计划中 | v1.2 |
| **HDFS** | `hdfsreader` | `hdfs-source.conf` | 📋 计划中 | v1.2 |
| **Kafka** | `kafkareader` | `kafka-source.conf` | 📋 计划中 | v1.3 |
| **MongoDB** | `mongoreader` | `mongodb-source.conf` | 📋 计划中 | v1.3 |
| **Elasticsearch** | `elasticsearchreader` | `elasticsearch-source.conf` | 📋 计划中 | v1.4 |
| **Redis** | `redisreader` | `redis-source.conf` | 📋 计划中 | v1.4 |

### 数据目标（Sinks）

| 数据目标类型 | DataX Writer | 模板文件 | 支持状态 | 备注 |
|-------------|-------------|----------|----------|------|
| **HDFS** | `hdfswriter` | `hdfs-sink.conf` | ✅ 完全支持 | 多种文件格式 |
| **Hive** | `hivewriter` | `hive-sink.conf` | 📋 计划中 | v1.2 |
| **MySQL** | `mysqlwriter` | `mysql-sink.conf` | 📋 计划中 | v1.2 |
| **PostgreSQL** | `postgresqlwriter` | `postgresql-sink.conf` | 📋 计划中 | v1.2 |
| **ClickHouse** | `clickhousewriter` | `clickhouse-sink.conf` | 🔧 开发中 | 高性能写入 |
| **Doris** | `doriswriter` | `doris-sink.conf` | 📋 计划中 | v1.3 |
| **Elasticsearch** | `elasticsearchwriter` | `elasticsearch-sink.conf` | 📋 计划中 | v1.3 |
| **Kafka** | `kafkawriter` | `kafka-sink.conf` | 📋 计划中 | v1.3 |
| **MongoDB** | `mongowriter` | `mongodb-sink.conf` | 📋 计划中 | v1.4 |
| **Redis** | `rediswriter` | `redis-sink.conf` | 📋 计划中 | v1.4 |

### 特殊功能

| 功能 | 描述 | 支持状态 |
|------|------|----------|
| **自动驱动映射** | 根据JDBC URL自动推断数据库驱动 | ✅ 已支持 |
| **智能查询生成** | 根据column、table、where自动生成SELECT语句 | ✅ 已支持 |
| **参数优化** | 自动设置连接池、分片等性能参数 | ✅ 已支持 |
| **批量转换** | 支持目录级别的批量配置转换 | ✅ 已支持 |
| **转换报告** | 生成详细的转换报告和参数映射说明 | ✅ 已支持 |

## 🎨 模板过滤器语法

X2SeaTunnel 支持强大的 Jinja2 风格模板语法，提供丰富的过滤器功能来处理配置转换。

### 基础语法

```bash
# 基本变量引用
{{ datax.job.content[0].reader.parameter.username }}

# 带过滤器的变量
{{ datax.job.content[0].reader.parameter.column | join(',') }}

# 链式过滤器
{{ datax.job.content[0].writer.parameter.path | split('/') | get(-2) | replace('.db','') }}
```

### 基础过滤器

#### 字符串操作
```bash
# 大小写转换
{{ value | upper }}              # 转换为大写
{{ value | lower }}              # 转换为小写

# 默认值设置
{{ value | default('默认值') }}   # 如果值为空则使用默认值
{{ datax.job.setting.speed.channel | default(1) }}  # 数值默认值
```

#### 数组操作
```bash
# 数组连接
{{ datax.job.content[0].reader.parameter.column | join(',') }}        # 用逗号连接
{{ datax.job.content[0].reader.parameter.column | join(' | ') }}      # 自定义分隔符
```

### 高级过滤器

#### 字符串分割和获取
```bash
# 分割字符串
{{ path | split('/') }}          # 按 '/' 分割字符串，返回数组

# 获取数组元素
{{ array | get(0) }}             # 获取第一个元素
{{ array | get(-1) }}            # 获取最后一个元素
{{ array | get(-2) }}            # 获取倒数第二个元素

# 字符串替换
{{ value | replace('old,new') }} # 将 'old' 替换为 'new'
```

#### 链式过滤器
```bash
# 从 HDFS 跄提取 Hive 表名
# 路径: /user/hive/warehouse/ecology_ods.db/ods_formtable_main/partition=20240101
{{ datax.job.content[0].writer.parameter.path | split('/') | get(-3) | replace('.db','') }}.{{ datax.job.content[0].writer.parameter.path | split('/') | get(-2) }}
# 结果: ecology_ods.ods_formtable_main

# 提取数据库名
{{ path | split('/') | get(-3) | replace('.db','') }}  # 去掉 .db 后缀

# 提取表名
{{ path | split('/') | get(-2) }}                      # 获取表名部分
```

### 正则表达式过滤器

```bash
# 正则提取
{{ value | regex_extract('pattern') }}                 # 提取匹配的第一个分组
{{ jdbcUrl | regex_extract('jdbc:mysql://([^:]+):') }} # 提取主机名

# 复杂正则提取示例
{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] | regex_extract('jdbc:([^:]+):') }}
# 从 JDBC URL 中提取数据库类型
```

### 转换器过滤器

#### JDBC 驱动映射
```bash
# 自动推断数据库驱动
{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] | jdbc_driver_mapper }}

# 映射关系（在 template-mapping.yaml 中配置）:
# mysql -> com.mysql.cj.jdbc.Driver
# postgresql -> org.postgresql.Driver
# oracle -> oracle.jdbc.driver.OracleDriver
# sqlserver -> com.microsoft.sqlserver.jdbc.SQLServerDriver
```

#### 自定义转换器
```bash
# 文件格式映射
{{ datax.job.content[0].writer.parameter.fileType | file_format_mapper }}

# 在 template-mapping.yaml 中配置:
# text -> text
# orc -> orc
# parquet -> parquet
```

### 实际应用示例

#### 1. 智能查询生成
```bash
# 自动生成 SQL 查询
query = "{{ datax.job.content[0].reader.parameter.querySql[0] | default('SELECT') }} {{ datax.job.content[0].reader.parameter.column | join(',') }} FROM {{ datax.job.content[0].reader.parameter.connection[0].table[0] }} WHERE {{ datax.job.content[0].reader.parameter.where | default('1=1') }}"

# 如果 DataX 配置中有 querySql，直接使用
# 否则根据 column、table、where 自动生成查询
```

#### 2. 路径智能解析
```bash
# 从复杂路径中提取信息
# 原始路径: /user/hive/warehouse/ecology_ods.db/ods_formtable_main/${partition}

# 提取数据库名
{% set database = datax.job.content[0].writer.parameter.path | split('/') | get(-3) | replace('.db','') %}

# 提取表名
{% set table = datax.job.content[0].writer.parameter.path | split('/') | get(-2) %}

# 组合使用
table_name = "{{ database }}.{{ table }}"
```

### 过滤器参考表

| 过滤器 | 语法 | 功能 | 示例 |
|--------|------|------|------|
| `upper` | `{{ value \| upper }}` | 转换为大写 | `hello → HELLO` |
| `lower` | `{{ value \| lower }}` | 转换为小写 | `HELLO → hello` |
| `default` | `{{ value \| default('默认值') }}` | 设置默认值 | `'' → 默认值` |
| `join` | `{{ array \| join(',') }}` | 数组连接 | `['a','b'] → 'a,b'` |
| `split` | `{{ string \| split('/') }}` | 字符串分割 | `'a/b/c' → ['a','b','c']` |
| `get` | `{{ array \| get(0) }}` | 获取数组元素 | `['a','b','c'] → 'a'` |
| `replace` | `{{ string \| replace('old,new') }}` | 字符串替换 | `'hello' → 'hallo'` |
| `regex_extract` | `{{ string \| regex_extract('pattern') }}` | 正则提取 | 提取匹配的内容 |
| `jdbc_driver_mapper` | `{{ jdbcUrl \| jdbc_driver_mapper }}` | JDBC 驱动映射 | 自动推断驱动类 |

### 高级技巧

#### 1. 嵌套过滤器
```bash
# 多层嵌套处理
{{ datax.job.content[0].reader.parameter.connection[0].jdbcUrl[0] | regex_extract('jdbc:([^:]+):') | jdbc_driver_mapper }}
```

#### 2. 条件过滤器
```bash
# 根据条件选择不同的过滤器
{{ value | default('') | upper if condition else value | lower }}
```

#### 3. 局部变量
```bash
# 使用局部变量简化复杂表达式
{% set base_path = datax.job.content[0].writer.parameter.path | split('/') %}
database = "{{ base_path | get(-3) | replace('.db','') }}"
table = "{{ base_path | get(-2) }}"
```

这些过滤器语法让你能够创建强大而灵活的配置转换模板，满足各种复杂的数据转换需求。

### 扩展指南

要添加新的数据源或目标类型，只需：

1. **创建模板文件**：在 `templates/datax/sources/` 或 `templates/datax/sinks/` 下创建模板
2. **配置映射**：在 `template-mapping.yaml` 中添加映射规则
3. **测试验证**：添加示例配置并进行转换测试

无需修改 Java 代码，完全通过配置驱动扩展。


## 🧪 测试用例和示例

### 示例用法
```bash
# 下面示例已在“基本用法”中列出，请参阅上方的示例并直接运行对应命令。
```

### 配置文件示例

#### DataX配置示例（MySQL到HDFS）
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "123456",
            "column": ["*"],
            "connection": [
              {
                "table": ["orders"],
                "jdbcUrl": ["jdbc:mysql://localhost:3306/ecommerce"]
              }
            ]
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "path": "/tmp/orders_output",
            "fileName": "orders",
            "writeMode": "truncate",
            "fieldDelimiter": "\t",
            "compress": "gzip"
          }
        }
      }
    ]
  }
}
```

#### 转换后的SeaTunnel配置示例
```hocon
env {
  execution.parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/ecommerce"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    query = "SELECT * FROM orders"
    result_table_name = "source_table"
  }
}

sink {
  File {
    path = "/tmp/orders_output"
    file_name_expression = "orders"
    file_format_type = "text"
    field_delimiter = "\t"
    compress_codec = "gzip"
    sink_columns = ["*"]
  }
}
```

#### 检查转换报告
转换完成后，查看生成的Markdown报告文件，包含：
- 详细的字段映射关系
- 自动构造的字段说明
- 可能的错误和警告信息


#### 日志文件
```bash
# 查看日志文件
tail -f logs/x2seatunnel.log
```


### 开发指南
#### 自定义配置模板

可以在 `templates/datax/custom/` 目录下自定义配置模板，参考现有模板的格式和占位符语法。

#### 代码结构

```
src/main/java/org/apache/seatunnel/tools/x2seatunnel/
├── cli/                    # 命令行界面
├── core/                   # 核心转换逻辑
├── template/               # 模板处理
├── utils/                  # 工具类
└── X2SeaTunnelApplication.java  # 主应用类
```

### 常见问题 (FAQ)

#### Q: 工具如何识别不同的JDBC数据源？
A: X2SeaTunnel通过以下方式识别JDBC数据源：
1. **Reader类型识别**：根据DataX配置中的`reader.name`字段（如`mysqlreader`、`postgresqlreader`等）
2. **URL协议分析**：解析`jdbcUrl`中的协议部分（如`jdbc:mysql:`、`jdbc:postgresql:`等）
3. **驱动自动映射**：使用`template-mapping.yaml`中的`jdbc_driver_mapper`自动选择正确的驱动类
4. **参数智能转换**：根据数据库类型应用特定的参数映射和优化配置

#### Q: 工具支持哪些数据库？
A: 目前工具支持MySQL、PostgreSQL、Oracle、SQL Server等关系型数据库，以及HDFS、Hive等大数据存储。完整的数据库支持列表请参考上方的"支持的数据源和目标类型"部分。

#### Q: 如何验证JDBC配置转换是否正确？
A: 可以通过以下方式验证：
1. 检查生成的配置文件中的`url`、`driver`、`user`、`query`等关键字段
2. 查看转换报告(`*.md`)中的参数映射详情
3. 使用`grep`命令快速检查关键配置项：`grep -E "(url|driver|partition_column)" output.conf`

#### Q: 转换后的配置文件可以直接使用吗？
A: 生成的配置文件是基于模板的标准配置，大多数情况下可以直接使用。复杂场景可能需要手动调整部分参数。

#### Q: 如何添加新的源配置类型？
A: 可以通过扩展映射配置文件和添加新的模板来支持新的源类型。详见开发指南。

#### Q: 转换报告包含哪些信息？
A: 转换报告包含转换状态、字段映射关系、参数转换详情、警告和错误信息等。

### 限制和注意事项

#### 当前版本限制
1. **转换功能**: 基于模板的配置转换，支持主流数据源和数据目标
2. **连接器映射**: 支持SeaTunnel主要连接器的映射
3. **参数转换**: 支持常用参数的自动转换和映射

#### 版本兼容性
- 支持 DataX 主流版本的配置格式
- 生成的配置兼容 SeaTunnel 2.3.12+ 版本
- 模板系统向后兼容


### 更新日志

#### v1.0.0-SNAPSHOT (当前版本)
- ✅ **核心功能**：支持DataX到SeaTunnel的基础配置转换
- ✅ **模板系统**：基于Jinja2风格的DSL模板语言，支持配置驱动扩展
- ✅ **JDBC统一支持**：MySQL、PostgreSQL、Oracle、SQL Server等关系型数据库
- ✅ **智能特性**：
  - 自动驱动映射（根据jdbcUrl推断数据库驱动）
  - 智能查询生成（根据column、table、where自动拼接SELECT语句）
  - 参数自动映射（splitPk→partition_column、fetchSize→fetch_size等）
- ✅ **模板语法**：
  - 基础变量访问：`{{ datax.path.to.value }}`
  - 过滤器支持：`{{ array | join(',') }}`、`{{ value | default('default') }}`
  - 自定义转换器：`{{ url | jdbc_driver_mapper }}`
- ✅ **批量处理**：支持目录级别的批量转换和报告生成
- ✅ **完整示例**：提供4种JDBC数据源的完整DataX配置样例
- ✅ **详细文档**：完整的使用说明和API文档

#### 计划功能 (未来版本)
- 🔮 **v1.1**：支持更多数据源类型（Hive、HDFS、ClickHouse）
- 🔮 **v1.2**：流式数据源支持（Kafka），性能优化
- 🔮 **v1.3**：NoSQL数据源支持（MongoDB、Redis、Elasticsearch）
- 🔮 **v1.4**：高级特性（配置验证、自动优化建议、兼容性检查）

