import ChangeLog from '../changelog/connector-file-adls.md';

# ADLSFile

> Azure Data Lake Storage Gen2 文件 Source 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] 批处理和流处理
- [x] 多模态文件读取
- [x] exactly-once、列投影和并行读取
- [x] text、csv、parquet、orc、json、excel、xml、binary、markdown、pdf

## 描述

通过 Hadoop ABFS 从 Azure Data Lake Storage Gen2 文件系统读取文件。`path` 可以指向单个文件，
也可以指向目录，并使用 `file_filter_pattern` 和递归扫描读取多个文件。

## 依赖

使用 SeaTunnel Zeta 时，ADLS runtime 模块会提供 Hadoop Azure 客户端及其依赖。使用 Spark 或
Flink 时，集群需要提供兼容的 Hadoop runtime。本连接器使用 Hadoop Azure 3.2.0 构建和测试。

## Source 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|------|------|------|--------|------|
| path | string | 是 | - | 要读取的 ADLS 文件或目录。 |
| file_format_type | enum | 是 | - | 输入文件格式。 |
| account_name | string | 是 | - | ADLS 存储账户名称。 |
| container | string | 是 | - | ADLS Gen2 文件系统（容器）名称。 |
| endpoint_suffix | string | 是 | `dfs.core.windows.net` | 存储端点后缀。 |
| auth_type | enum | 是 | `SHARED_KEY` | `SHARED_KEY` 或 `OAUTH_CLIENT_CREDENTIALS`。 |
| account_key | string | 条件必填 | - | 使用共享密钥认证时必填。 |
| tenant_id | string | 条件必填 | - | Microsoft Entra 租户 ID。 |
| client_id | string | 条件必填 | - | Microsoft Entra 应用程序 ID。 |
| client_secret | string | 条件必填 | - | Microsoft Entra 客户端密钥。 |
| authority_host | string | 否 | `https://login.microsoftonline.com` | OAuth 客户端凭据认证使用的 HTTPS authority 源站。 |
| hadoop_adls_properties | map | 否 | - | 由可信任的操作人员提供的额外 Hadoop ABFS 属性。参见下方的 ADLS 配置规则。 |
| file_filter_pattern | string | 否 | - | 选择文件的正则表达式。 |
| recursive_file_scan | boolean | 否 | `true` | 是否扫描嵌套目录。 |
| parse_partition_from_path | boolean | 否 | `true` | 从 `key=value` 路径片段解析分区字段。 |
| read_columns | list | 否 | - | 要读取的字段列表。 |
| schema | object | 条件必填 | - | text、JSON、Excel、CSV 和 XML 输入的行 Schema。 |
| field_delimiter | string | 否 | `\001` | 文本字段分隔符。 |
| row_delimiter | string | 否 | `\n` | text 输入的行分隔符。 |
| skip_header_row_number | long | 否 | `0` | 跳过的表头行数。 |
| encoding | string | 否 | `UTF-8` | 输入字符编码。 |
| enable_file_split | boolean | 否 | `false` | 是否拆分支持的超大文件。 |
| file_split_size | long | 条件必填 | `134217728` | 启用文件拆分时的拆分大小（字节）。 |
| discovery_mode | enum | 否 | `ONCE` | 文件发现模式。 |
| scan_interval | duration | 否 | `10S` | 连续发现时的扫描间隔。 |
| start_mode | enum | 否 | `EARLIEST` | 连续发现的起始模式。 |
| sync_mode | enum | 否 | `FULL` | 全量或更新同步模式。 |
| post_sync_action | enum | 否 | `NONE` | 文件同步后的动作。 |
| target_path | string | 条件必填 | - | `sync_mode=UPDATE` 时用于比较的目标路径。 |
| target_hadoop_conf | map | 否 | - | 更新比较目标的 Hadoop 配置。 |
| update_strategy | enum | 否 | `DISTCP` | 更新比较策略：`DISTCP` 或 `STRICT`。 |
| compare_mode | enum | 否 | `LEN_MTIME` | 按长度与修改时间比较；`STRICT` 模式也可使用校验和。 |
| update_compare_parallelism | int | 否 | `8` | 目标元数据查询的并行度。 |
| update_compare_bulk_threshold | int | 否 | `0` | 触发目录列表比较的候选数量；零表示禁用。 |
| backup_path | string | 条件必填 | - | `post_sync_action=BACKUP` 时的备份目录。 |
| retention_max_age | duration | 否 | - | 备份文件清理前的最大保留时间。 |
| retention_check_interval | duration | 否 | `1H` | 备份保留期的扫描间隔。 |
| null_format | string | 否 | - | 表示空值的字符串。 |
| quote_char | string | 否 | `"` | CSV 字段的引号字符。 |
| escape_char | string | 否 | - | CSV 字段的转义字符。 |
| sheet_name | string | 否 | - | 要读取的 Excel 工作表。 |
| excel_engine | enum | 否 | `POI` | Excel 读取引擎：`POI` 或 `EasyExcel`。 |
| poi_excel_max_file_size | long | 否 | `52428800` | POI 可读取的最大 Excel 文件大小（字节）。 |
| compress_codec | enum | 否 | `NONE` | 输入文件压缩格式。 |
| archive_compress_codec | enum | 否 | `NONE` | 归档文件压缩格式。 |
| xml_row_tag | string | 条件必填 | - | XML 文件的数据行标签。 |
| xml_use_attr_format | boolean | 条件必填 | - | 是否从 XML 属性读取数据。 |
| markdown_rag_metadata_enabled | boolean | 否 | `false` | 读取 Markdown 时附加 RAG 元数据。 |
| pdf_rag_metadata_enabled | boolean | 否 | `false` | 读取 PDF 时附加 RAG 元数据。 |
| filename_extension | string | 否 | - | 按文件扩展名过滤。 |
| date_format | string | 否 | `yyyy-MM-dd` | 日期解析格式。 |
| datetime_format | string | 否 | `yyyy-MM-dd HH:mm:ss` | 日期时间解析格式。 |
| time_format | string | 否 | `HH:mm:ss` | 时间解析格式。 |
| metalake_type | enum | 否 | `GRAVITINO` | 用于解析表结构的元数据湖类型。 |
| common-options | object | 否 | - | 参见 [Source Common Options](../common-options/source-common-options.md)。 |

### ADLS 配置规则

`account_name` 只能包含 3–24 个小写字母或数字。`container` 长度为 3–63 个字符，必须以小写字母或
数字开头和结尾，只能使用小写字母、数字和不连续的连字符。`endpoint_suffix` 必须是没有协议和路径的
DNS 后缀。

两种认证方式互斥：`SHARED_KEY` 要求 `account_key`，不允许设置 `tenant_id`、`client_id` 和
`client_secret`；`OAUTH_CLIENT_CREDENTIALS` 要求后三项，不允许设置 `account_key`。
`tenant_id` 必须是 GUID 或 DNS 名称。`authority_host` 必须是没有路径、查询参数和片段的 HTTPS
地址（允许末尾有一个 `/`）。

`authority_host` 是信任边界：校验仅检查 URL 语法，不限制目标主机。OAuth 会将 `client_id` 和
`client_secret` 发送到该源站下的令牌端点。仅应由可信任的操作人员设置此选项，并使用其 Azure 云对应的
Microsoft Entra authority。不要从不可信任的作业输入中接受此配置。

`hadoop_adls_properties` 中的属性键在去除首尾空白字符后必须唯一。例如，`"k"` 和 `" k "`
会导致校验失败，即使它们的值相同。请为每个去除首尾空白后的键只保留一个配置项。
属性键保留大小写，因此仅大小写不同的键仍被视为不同的键。

`hadoop_adls_properties` 中每个属性键和值在传递给 Hadoop 之前，都会去除首尾空白字符。
去除空白后为空的键会被拒绝。键名的其他字符和大小写保持不变；拒绝列表匹配不区分大小写。

`hadoop_adls_properties` 可用于 ABFS 参数调整。不允许 `fs.defaultFS`，以及以下前缀开头的键：
`fs.abfs`、`fs.azure.account.auth.type`、`fs.azure.account.key`、
`fs.azure.account.oauth`、`fs.azure.sas.`、`fs.azure.delegation.`、
`fs.azure.enable.delegation.token`、`fs.azure.identity.`、`fs.azure.shellkeyprovider.`、
`abfs.external.authorization.class`、`fs.azure.abfs.endpoint`、`fs.azure.custom.token.provider.class`、
`fs.azure.encryption.context.provider.type`、
`fs.azure.encryption.encoded.client-provided-key`、
`fs.azure.metrics.account.key`、`hadoop.security.credential.provider.path`。
匹配时不区分大小写；这些路由、凭据和提供程序设置由连接器管理。

校验采用键名完全匹配或前缀匹配的拒绝列表。该列表并不完整：其他 Hadoop 配置仍可能加载类，或选择
凭据/令牌提供程序。`hadoop_adls_properties` 仅应由可信任的操作人员提供，不要从不可信任的作业输入中
接受此配置映射。

解析配置时，日志会自动遮盖 `account_key` 和 `client_secret`。这只是日志遮盖，不是配置加密。
`account_key` 是 core-starter 的全局日志遮盖关键字，也会影响其他使用同名选项的连接器。

## 示例

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  ADLSFile {
    path = "/landing/input"
    file_filter_pattern = ".*\\.csv"
    recursive_file_scan = true
    account_name = "mystorageaccount"
    container = "data"
    auth_type = "SHARED_KEY"
    account_key = ${?ADLS_ACCOUNT_KEY}
    file_format_type = "csv"
    skip_header_row_number = 1
    schema { fields { id = bigint, name = string, amount = double } }
  }
}

sink {
  Console { }
}
```

使用 Microsoft Entra 客户端凭据时，将 `auth_type` 设置为 `OAUTH_CLIENT_CREDENTIALS`，并提供
`tenant_id`、`client_id` 和 `client_secret`。

## 实时集成测试

`seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-adls-e2e` 中的 `ADLSFileIT`
通过安全 ABFS（`abfss://`）访问真实 ADLS Gen2 存储，测试 ADLSFile Source 和 Sink。
默认 CI 使用独立的 WASB/Azurite 测试；实时测试需要 Azure 资源和凭据，因此必须显式启用。
Azurite 测试通过并不代表已验证真实 ABFS/Gen2 I/O。

### 1. 准备 Azure 存储和本地环境

- 使用专用的[已启用分层命名空间的 ADLS Gen2 存储账户](https://learn.microsoft.com/zh-cn/azure/storage/blobs/create-data-lake-storage-account)，
  并创建用于测试的文件系统（容器）。
- 获取[存储账户访问密钥](https://learn.microsoft.com/zh-cn/azure/storage/common/storage-account-keys-manage)。
  使用密钥值，而不是连接字符串或 SAS 令牌。测试账户必须允许 Shared Key 授权。
  现有实时测试使用 `SHARED_KEY`，不测试 OAuth 客户端凭据认证。
- 测试配置使用 `dfs.core.windows.net`，因此本步骤适用于 Azure 公有云。
  运行 Maven 的主机和 Docker 容器都必须能够访问存储端点，账户防火墙规则也必须允许该访问。
- 安装 JDK 11 或 17，设置 `JAVA_HOME`，并启动启用 Linux 容器的 Docker。
  构建前提条件参见[开发环境配置指南](../../developer/setup.md)。

选择专用的容器内绝对路径前缀，例如 `/seatunnel-e2e/alice/run-001`。
测试会在初始化前和清理阶段递归删除 `<prefix>/file-round-trip-e2e`。
不要在此目录存放已有数据，并发测试必须使用不同的前缀。

### 2. 构建当前源码

在仓库根目录运行以下命令，并在构建完成后再设置实时测试环境变量：

```bash
./mvnw clean install -DskipUT=true -DskipIT=true
```

Windows PowerShell 用户应将此处和后续 Maven 命令中的 `./mvnw` 替换为 `./mvnw.cmd`。
构建会准备测试容器需要的连接器、runtime、引擎和 E2E 依赖。

### 3. 设置实时测试环境变量

在稍后运行 Maven 的同一个终端中设置以下变量：

| 变量 | 值 |
|------|----|
| `SEATUNNEL_ADLS_IT` | 设置为 `true` 以启用 `ADLSFileIT`。 |
| `SEATUNNEL_ADLS_ACCOUNT` | 存储账户名称，不包含 URL 或端点后缀。 |
| `SEATUNNEL_ADLS_CONTAINER` | 已创建的测试文件系统（容器）名称。 |
| `SEATUNNEL_ADLS_ACCOUNT_KEY` | 存储账户访问密钥。 |
| `SEATUNNEL_ADLS_TEST_PREFIX` | 容器内的专用绝对路径，不能包含 `.` 或 `..` 路径段。 |
| `RUN_ENGINES` | 本示例使用 `zeta`。 |

Bash（请替换示例中的账户、容器和路径前缀）：

```bash
export SEATUNNEL_ADLS_IT=true
export SEATUNNEL_ADLS_ACCOUNT=mystorageaccount
export SEATUNNEL_ADLS_CONTAINER=seatunnel-tests
export SEATUNNEL_ADLS_TEST_PREFIX=/seatunnel-e2e/alice/run-001
export RUN_ENGINES=zeta
read -r -s -p 'Storage account access key: ' SEATUNNEL_ADLS_ACCOUNT_KEY
printf '\n'
export SEATUNNEL_ADLS_ACCOUNT_KEY
```

Windows PowerShell：

```powershell
$env:SEATUNNEL_ADLS_IT = 'true'
$env:SEATUNNEL_ADLS_ACCOUNT = 'mystorageaccount'
$env:SEATUNNEL_ADLS_CONTAINER = 'seatunnel-tests'
$env:SEATUNNEL_ADLS_TEST_PREFIX = '/seatunnel-e2e/alice/run-001'
$env:RUN_ENGINES = 'zeta'
$adlsTestKey = Read-Host 'Storage account access key' -AsSecureString
$env:SEATUNNEL_ADLS_ACCOUNT_KEY = [System.Net.NetworkCredential]::new('', $adlsTestKey).Password
Remove-Variable adlsTestKey
```

交互式输入可以避免将密钥写入命令历史。CI 应通过密钥存储注入凭据。
测试会将账户、容器、密钥和生成的测试根目录传递给引擎容器；执行期间不需要交互式登录 Azure 门户。

### 4. 运行实时测试

在同一个终端的仓库根目录执行：

```bash
./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-adls-e2e -DskipUT=true -DskipIT=false -Dit.test=ADLSFileIT verify
```

设置 `RUN_ENGINES=spark` 可测试符合条件的 Spark 容器，设置为 `all` 可测试所有符合条件的容器。
测试会排除 Spark 2.4 和 Flink 容器，因为它们的 Hadoop runtime 与本连接器所需的 Hadoop 3 Azure
runtime 不兼容。

### 5. 检查结果并清理

测试会创建包含四条记录的两个输入 CSV 文件，执行 ADLSFile Source 到 Sink 的作业，验证输出创建、
旧输出文件删除以及临时文件清理，然后通过 ADLSFile 回读输出并交给 Assert Sink。
Assert Sink 检查记录数为四、字段非空以及 ID 范围。清理阶段会删除生成的测试根目录。

在 `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-adls-e2e/target/failsafe-reports/`
查看 `ADLSFileIT` 的文本或 XML 报告。确认测试实际执行且没有失败或错误；
仅显示跳过测试或构建成功，不能证明已验证真实 ABFS 访问。
如果测试被跳过，请检查 `SEATUNNEL_ADLS_IT` 和 `-DskipIT=false`。
如果认证或连接失败，请检查账户和密钥、Shared Key 授权、容器名称以及网络访问。

如果进程中断且清理未完成，请检查并仅删除本次测试专用的 `<prefix>/file-round-trip-e2e` 目录。
完成后关闭终端或移除测试环境变量，尤其是 `SEATUNNEL_ADLS_ACCOUNT_KEY` 和 `SEATUNNEL_ADLS_IT`。

## 变更日志

ADLS Gen2 文件 Source 初始文档。

<ChangeLog />
