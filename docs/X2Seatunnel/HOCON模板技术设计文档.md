# X2SeaTunnel 基于HOCON模板的技术设计文档

## 概述

本文档详细描述了X2SeaTunnel工具基于HOCON模板和占位符语法的技术设计方案。该方案采用"拉取式"映射思想，以SeaTunnel原生配置格式为模板，通过占位符语法实现配置驱动的转换。

## 设计原则

### 1. 模板驱动
- 使用SeaTunnel原生HOCON配置格式作为模板
- 用户直接看到最终的配置效果
- 无需学习额外的映射配置语法

### 2. Source/Sink分离
- 模板按连接器类型分离，不按组合创建
- 任意Source和Sink可以自由组合
- 模板数量从N×M减少到N+M

### 3. 多工具支持
- 不同数据同步工具使用独立的模板目录
- 每个工具有专用的占位符语法
- 工具间完全隔离，互不影响

### 4. 占位符语法
- 使用 `${tool:json_path}` 语法标记数据来源
- 支持默认值：`${tool:json_path|default_value}`
- 支持嵌套占位符和条件处理

### 5. 配置驱动扩展
- 新增连接器支持仅需创建模板文件
- 支持热更新，无需重新编译
- 配置文件版本控制和管理

## 架构设计

### 目录结构
```
config/x2seatunnel/
├── templates/                          # 模板目录
│   ├── datax/                          # DataX专用模板
│   │   ├── sources/                    # DataX Source连接器模板
│   │   │   ├── mysql-jdbc-source.conf  # MySQL JDBC Source模板
│   │   │   ├── postgresql-jdbc-source.conf # PostgreSQL JDBC Source模板
│   │   │   ├── oracle-jdbc-source.conf # Oracle JDBC Source模板
│   │   │   ├── hdfs-source.conf        # HDFS Source模板
│   │   │   └── generic-jdbc-source.conf # 通用JDBC Source模板
│   │   ├── sinks/                      # DataX Sink连接器模板
│   │   │   ├── hive-sink.conf          # Hive Sink模板
│   │   │   ├── hdfs-sink.conf          # HDFS Sink模板
│   │   │   ├── clickhouse-sink.conf    # ClickHouse Sink模板
│   │   │   ├── doris-sink.conf         # Doris Sink模板
│   │   │   └── generic-sink.conf       # 通用Sink模板
│   │   └── env/                        # DataX环境配置模板
│   │       ├── batch-env.conf          # 批处理环境配置
│   │       └── streaming-env.conf      # 流处理环境配置
│   ├── sqoop/                          # Sqloop专用模板（未来扩展）
│   │   ├── sources/                    # Sqoop Source连接器模板
│   │   ├── sinks/                      # Sqoop Sink连接器模板
│   │   └── env/                        # Sqoop环境配置模板
│   └── flume/                          # Flume专用模板（未来扩展）
│       ├── sources/                    # Flume Source连接器模板
│       ├── sinks/                      # Flume Sink连接器模板
│       └── env/                        # Flume环境配置模板
├── connector-mapping.yaml              # 连接器映射配置
├── placeholder-rules.yaml              # 占位符处理规则
├── conversion-config.yaml              # 转换引擎配置
└── template-versions.yaml              # 模板版本控制
```

### 核心组件

#### 1. ToolIdentifier
负责识别源配置文件的工具类型。

```java
public class ToolIdentifier {
    
    /**
     * 根据配置文件内容识别工具类型
     */
    public ToolType identifyTool(String configContent) {
        JsonNode config = parseConfig(configContent);
        
        // DataX特征识别
        if (config.has("job") && config.get("job").has("content")) {
            return ToolType.DATAX;
        }
        
        // Sqoop特征识别
        if (config.has("connection") && config.has("table")) {
            return ToolType.SQOOP;
        }
        
        // Flume特征识别
        if (config.has("sources") && config.has("sinks") && config.has("channels")) {
            return ToolType.FLUME;
        }
        
        throw new UnsupportedToolException("Unknown tool type");
    }
}
```

#### 2. TemplateMappingResolver
负责根据工具类型和连接器组合选择合适的模板文件。

```java
public class TemplateMappingResolver {
    
    /**
     * 根据工具类型和连接器配置选择模板文件
     */
    public TemplateSet resolveTemplates(ToolType toolType, Object sourceConfig) {
        switch (toolType) {
            case DATAX:
                return resolveDataXTemplates((DataXConfig) sourceConfig);
            case SQOOP:
                return resolveSqoopTemplates((SqoopConfig) sourceConfig);
            case FLUME:
                return resolveFlumeTemplates((FlumeConfig) sourceConfig);
            default:
                throw new UnsupportedOperationException("Unsupported tool: " + toolType);
        }
    }
    
    private TemplateSet resolveDataXTemplates(DataXConfig config) {
        String readerName = config.getReaderName();
        String writerName = config.getWriterName();
        
        // 从connector-mapping.yaml中获取模板路径
        String sourceTemplate = getMappingConfig().getDataX().getSourceMappings().get(readerName);
        String sinkTemplate = getMappingConfig().getDataX().getSinkMappings().get(writerName);
        String envTemplate = getMappingConfig().getDataX().getEnvMappings().get("batch");
        
        return new TemplateSet(sourceTemplate, sinkTemplate, envTemplate);
    }
}
```

#### 3. PlaceholderProcessor
负责处理模板中的占位符替换。

```java
public class PlaceholderProcessor {
    
    // 不同工具的占位符模式
    private static final Map<ToolType, Pattern> PLACEHOLDER_PATTERNS = Map.of(
        ToolType.DATAX, Pattern.compile("\\$\\{datax:([^|}]+)(\\|([^}]*))?\\}"),
        ToolType.SQOOP, Pattern.compile("\\$\\{sqoop:([^|}]+)(\\|([^}]*))?\\}"),
        ToolType.FLUME, Pattern.compile("\\$\\{flume:([^|}]+)(\\|([^}]*))?\\}")
    );
    
    /**
     * 处理模板中的占位符
     */
    public String processTemplate(String template, ToolType toolType, JsonNode sourceConfig) {
        Pattern pattern = PLACEHOLDER_PATTERNS.get(toolType);
        if (pattern == null) {
            throw new UnsupportedOperationException("Unsupported tool type: " + toolType);
        }
        
        return pattern.matcher(template).replaceAll(match -> {
            String jsonPath = match.group(1);
            String defaultValue = match.group(3);
            
            return extractValue(sourceConfig, jsonPath, defaultValue, toolType);
        });
    }
    
    private String extractValue(JsonNode config, String path, String defaultValue, ToolType toolType) {
        try {
            // 根据工具类型选择不同的路径解析策略
            JsonNode value = extractValueByTool(config, path, toolType);
            if (value != null && !value.isNull()) {
                return processValue(value.asText());
            }
        } catch (Exception e) {
            logger.warn("Failed to extract value from path: {} for tool: {}", path, toolType);
        }
        
        return defaultValue != null ? defaultValue : "";
    }
    
    private JsonNode extractValueByTool(JsonNode config, String path, ToolType toolType) {
        switch (toolType) {
            case DATAX:
                return JsonPath.read(config, path);
            case SQOOP:
                return extractSqoopValue(config, path);
            case FLUME:
                return extractFlumeValue(config, path);
            default:
                throw new UnsupportedOperationException("Unsupported tool: " + toolType);
        }
    }
}
```

#### 4. TemplateAssembler
负责组装完整的SeaTunnel配置。

```java
public class TemplateAssembler {
    
    /**
     * 组装完整的SeaTunnel配置
     */
    public String assembleConfiguration(TemplateSet templates, ToolType toolType, JsonNode sourceConfig) {
        StringBuilder configBuilder = new StringBuilder();
        
        // 1. 添加环境配置
        String envContent = loadTemplate(templates.getEnvTemplate());
        String processedEnv = placeholderProcessor.processTemplate(envContent, toolType, sourceConfig);
        configBuilder.append(processedEnv).append("\n\n");
        
        // 2. 添加Source配置
        String sourceContent = loadTemplate(templates.getSourceTemplate());
        String processedSource = placeholderProcessor.processTemplate(sourceContent, toolType, sourceConfig);
        configBuilder.append("source {\n").append(processedSource).append("\n}\n\n");
        
        // 3. 添加Sink配置
        String sinkContent = loadTemplate(templates.getSinkTemplate());
        String processedSink = placeholderProcessor.processTemplate(sinkContent, toolType, sourceConfig);
        configBuilder.append("sink {\n").append(processedSink).append("\n}\n");
        
        return configBuilder.toString();
    }
    
    private String loadTemplate(String templatePath) {
        try {
            return Files.readString(Paths.get("config/x2seatunnel/templates/" + templatePath));
        } catch (IOException e) {
            throw new TemplateLoadException("Failed to load template: " + templatePath, e);
        }
    }
}
```

#### 5. ValueTransformer
负责处理特殊的值转换逻辑。

```java
public interface ValueTransformer {
    String transform(String value, Map<String, Object> context);
}

public class FileTypeMapper implements ValueTransformer {
    private static final Map<String, String> TYPE_MAPPINGS = Map.of(
        "text", "text",
        "orc", "orc",
        "parquet", "parquet",
        "avro", "avro",
        "csv", "text",
        "json", "json"
    );
    
    @Override
    public String transform(String value, Map<String, Object> context) {
        return TYPE_MAPPINGS.getOrDefault(value.toLowerCase(), "parquet");
    }
}
```

#### 5. ConfigurationValidator
负责验证生成的SeaTunnel配置。

```java
public class ConfigurationValidator {
    
    /**
     * 验证SeaTunnel配置的完整性和正确性
     */
    public ValidationResult validate(String seaTunnelConfig) {
        ValidationResult result = new ValidationResult();
        
        // 1. HOCON语法验证
        validateHoconSyntax(seaTunnelConfig, result);
        
        // 2. 必填字段验证
        validateRequiredFields(seaTunnelConfig, result);
        
        // 3. 字段格式验证
        validateFieldFormats(seaTunnelConfig, result);
        
        return result;
    }
}
```

## 配置文件规范

### 1. 连接器映射配置 (connector-mapping.yaml)
```yaml
# 连接器映射配置 - 按工具分离
# 每个工具使用独立的映射规则，避免相互影响

# DataX连接器映射
datax:
  source_mappings:
    # DataX Reader名称 -> SeaTunnel Source模板文件
    "mysqlreader": "datax/sources/mysql-jdbc-source.conf"
    "postgresqlreader": "datax/sources/postgresql-jdbc-source.conf"
    "oraclereader": "datax/sources/oracle-jdbc-source.conf"
    "hdfsreader": "datax/sources/hdfs-source.conf"
    "streamreader": "datax/sources/stream-source.conf"
    
  sink_mappings:
    # DataX Writer名称 -> SeaTunnel Sink模板文件
    "hivewriter": "datax/sinks/hive-sink.conf"
    "hdfswriter": "datax/sinks/hdfs-sink.conf"
    "mysqlwriter": "datax/sinks/mysql-jdbc-sink.conf"
    "postgresqlwriter": "datax/sinks/postgresql-jdbc-sink.conf"
    "clickhousewriter": "datax/sinks/clickhouse-sink.conf"
    "doriswriter": "datax/sinks/doris-sink.conf"
    "elasticsearchwriter": "datax/sinks/elasticsearch-sink.conf"

  env_mappings:
    # DataX作业模式 -> 环境配置模板
    "batch": "datax/env/batch-env.conf"
    "streaming": "datax/env/streaming-env.conf"
    
  defaults:
    source_template: "datax/sources/generic-jdbc-source.conf"
    sink_template: "datax/sinks/generic-sink.conf"
    env_template: "datax/env/batch-env.conf"

# Sqoop连接器映射（未来扩展）
sqoop:
  source_mappings:
    # Sqoop数据源类型 -> SeaTunnel Source模板文件
    "mysql": "sqoop/sources/mysql-jdbc-source.conf"
    "postgresql": "sqoop/sources/postgresql-jdbc-source.conf"
    "oracle": "sqoop/sources/oracle-jdbc-source.conf"
    "hdfs": "sqoop/sources/hdfs-source.conf"
    
  sink_mappings:
    # Sqoop目标类型 -> SeaTunnel Sink模板文件
    "hive": "sqoop/sinks/hive-sink.conf"
    "hdfs": "sqoop/sinks/hdfs-sink.conf"
    "mysql": "sqoop/sinks/mysql-jdbc-sink.conf"
    
  env_mappings:
    "import": "sqoop/env/import-env.conf"
    "export": "sqoop/env/export-env.conf"
    
  defaults:
    source_template: "sqoop/sources/generic-jdbc-source.conf"
    sink_template: "sqoop/sinks/generic-sink.conf"
    env_template: "sqoop/env/import-env.conf"

# Flume连接器映射（未来扩展）
flume:
  source_mappings:
    # Flume Source类型 -> SeaTunnel Source模板文件
    "spooldir": "flume/sources/file-source.conf"
    "kafka": "flume/sources/kafka-source.conf"
    "hdfs": "flume/sources/hdfs-source.conf"
    
  sink_mappings:
    # Flume Sink类型 -> SeaTunnel Sink模板文件
    "hdfs": "flume/sinks/hdfs-sink.conf"
    "kafka": "flume/sinks/kafka-sink.conf"
    "elasticsearch": "flume/sinks/elasticsearch-sink.conf"

# 模板搜索路径（按优先级排序）
template_search_paths:
  - "config/x2seatunnel/templates/"           # 项目根目录模板
  - "classpath:templates/"                     # 内置模板（JAR包内）

# 模板缓存配置
cache_config:
  enabled: true
  max_size: 100
  expire_after_access: "30m"
  expire_after_write: "1h"
```

### 2. 占位符处理规则 (placeholder-rules.yaml)
```yaml
# 占位符语法配置 - 按工具分离
# 每个工具使用专用的占位符语法

# DataX占位符配置
datax:
  placeholder_syntax:
    prefix: "${"           # 占位符前缀
    suffix: "}"            # 占位符后缀
    source_prefix: "datax:" # 数据源标识符
    default_separator: "|" # 默认值分隔符
    transformer_prefix: "@" # 转换器标识符
    
  # DataX特殊处理规则
  processing_rules:
    # 数组处理：自动取第一个元素
    array_auto_first:
      pattern: "\\[0\\]$"
      action: "take_first_element"
      description: "自动提取数组的第一个元素"
      
    # 数组处理：连接所有元素
    array_join:
      pattern: "\\[\\*\\]$"
      action: "join_elements"
      separator: ","
      description: "将数组元素连接成字符串"

# Sqoop占位符配置
sqoop:
  placeholder_syntax:
    prefix: "${"
    suffix: "}"
    source_prefix: "sqoop:"
    default_separator: "|"
    transformer_prefix: "@"
    
  # Sqoop特殊处理规则
  processing_rules:
    # Sqoop命令行参数处理
    command_line_args:
      pattern: "args\\."
      action: "extract_command_arg"
      description: "从Sqoop命令行参数中提取值"

# Flume占位符配置
flume:
  placeholder_syntax:
    prefix: "${"
    suffix: "}"
    source_prefix: "flume:"
    default_separator: "|"
    transformer_prefix: "@"
    
  # Flume特殊处理规则
  processing_rules:
    # Flume配置层级处理
    config_hierarchy:
      pattern: "\\w+\\."
      action: "resolve_hierarchy"
      description: "解析Flume配置层级结构"

# 通用值转换器定义
transformers:
  # 文件类型映射转换器
  file_type_mapper:
    type: "value_mapping"
    description: "文件类型到SeaTunnel文件类型的映射"
    mappings:
      "text": "text"
      "orc": "orc"
      "parquet": "parquet"
      "avro": "avro"
      "csv": "text"
      "json": "json"
      "excel": "excel"
    default: "parquet"
    case_sensitive: false
    
  # 压缩格式映射转换器
  compress_mapper:
    type: "value_mapping"
    description: "压缩格式映射"
    mappings:
      "gzip": "gzip"
      "bzip2": "bzip2"
      "snappy": "snappy"
      "lzo": "lzo"
      "lz4": "lz4"
      "zstd": "zstd"
      "none": "none"
      "": "none"
    default: "none"
    case_sensitive: false
    
  # 写入模式映射转换器
  write_mode_mapper:
    type: "value_mapping"
    description: "写入模式映射"
    mappings:
      "append": "append"
      "overwrite": "overwrite"
      "truncate": "overwrite"
      "ignore": "ignore"
      "errorifexists": "error"
    default: "append"
    case_sensitive: false
    
  # 数据库驱动映射转换器
  jdbc_driver_mapper:
    type: "value_mapping"
    description: "JDBC驱动类映射"
    mappings:
      "mysql": "com.mysql.cj.jdbc.Driver"
      "postgresql": "org.postgresql.Driver"
      "oracle": "oracle.jdbc.driver.OracleDriver"
      "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      "clickhouse": "ru.yandex.clickhouse.ClickHouseDriver"
    default: "com.mysql.cj.jdbc.Driver"

# 特殊处理规则
processing_rules:
  # 数组处理：自动取第一个元素
  array_auto_first:
    pattern: "\\[0\\]$"
    action: "take_first_element"
    description: "自动提取数组的第一个元素"
    
  # 数组处理：连接所有元素
  array_join:
    pattern: "\\[\\*\\]$"
    action: "join_elements"
    separator: ","
    description: "将数组元素连接成字符串"
    
  # 空值处理
  null_value_handling:
    pattern: "\\|\\s*$"
    action: "use_empty_string"
    description: "将null值转换为空字符串"
    
  # 嵌套占位符处理
  nested_placeholder:
    pattern: "\\$\\{[^}]+\\}"
    action: "recursive_resolve"
    max_depth: 3
    description: "递归解析嵌套的占位符"

# 验证规则
validation_rules:
  # 必填字段验证
  required_fields:
    source:
      - "url"
      - "result_table_name"
    sink:
      - "path"
      
  # 字段格式验证
  field_formats:
    url:
      pattern: "^jdbc:.*"
      message: "URL must be a valid JDBC URL"
      
    parallelism:
      type: "integer"
      min: 1
      max: 100
      message: "Parallelism must be between 1 and 100"
      
  # 字段依赖验证
  field_dependencies:
    - if_field: "file_format"
      if_value: "parquet"
      then_required: ["compress_codec"]
      message: "Parquet format requires compress_codec to be specified"
```

### 3. 转换引擎配置 (conversion-config.yaml)
```yaml
# 转换引擎配置
engine_config:
  # 处理器配置
  processors:
    template_resolver:
      class: "org.apache.seatunnel.tools.x2seatunnel.core.TemplateMappingResolver"
      cache_enabled: true
      cache_size: 100
      cache_ttl: "30m"
      
    template_composer:
      class: "org.apache.seatunnel.tools.x2seatunnel.core.TemplateComposer"
      preserve_formatting: true
      
    placeholder_processor:
      class: "org.apache.seatunnel.tools.x2seatunnel.core.PlaceholderProcessor"
      recursive_depth: 3
      fail_on_missing: false
      enable_escaping: true
      
    config_validator:
      class: "org.apache.seatunnel.tools.x2seatunnel.core.ConfigurationValidator"
      strict_mode: false
      validate_syntax: true
      validate_semantics: true
      
    report_generator:
      class: "org.apache.seatunnel.tools.x2seatunnel.core.ReportGenerator"
      detailed_mode: true
      include_warnings: true

  # 错误处理配置
  error_handling:
    on_template_not_found: "use_fallback"      # use_fallback, throw_error, generate_basic
    on_placeholder_error: "use_default"        # use_default, throw_error, skip
    on_validation_error: "warn_and_continue"   # warn_and_continue, throw_error, ignore
    on_transformer_error: "use_default"        # use_default, throw_error, skip
    
  # 输出配置
  output:
    format: "hocon"           # hocon, json, yaml
    indent: 2                 # 缩进空格数
    include_comments: true    # 是否包含注释
    preserve_order: true      # 是否保持字段顺序
    line_separator: "\n"      # 行分隔符

# 日志配置
logging:
  level: "INFO"
  include_transformation_details: true
  log_placeholder_replacements: true
  log_template_selection: true
  log_template_composition: true
  log_validation_results: true
```

## SeaTunnel配置模板示例

### 1. DataX MySQL JDBC Source模板 (datax/sources/mysql-jdbc-source.conf)
```hocon
# DataX MySQL JDBC Source连接器模板
# 使用DataX专用的占位符语法从DataX配置中提取数据
Jdbc {
  # 数据库连接配置 - DataX专用路径
  url = "${datax:job.content[0].reader.parameter.connection[0].jdbcUrl[0]}"
  driver = "com.mysql.cj.jdbc.Driver"
  user = "${datax:job.content[0].reader.parameter.username}"
  password = "${datax:job.content[0].reader.parameter.password|}"
  
  # 查询配置 - 支持自定义SQL或自动生成
  query = "${datax:job.content[0].reader.parameter.querySql[0]|SELECT ${datax:job.content[0].reader.parameter.column[*]|*} FROM ${datax:job.content[0].reader.parameter.connection[0].table[0]}}"
  
  # 数据分割配置（可选）- DataX专用参数
  partition_column = "${datax:job.content[0].reader.parameter.splitPk|}"
  partition_num = ${datax:job.setting.speed.channel|1}
  
  # 连接池配置
  connection_check_timeout_sec = 60
  
  # 结果表名
  result_table_name = "source_table"
  
  # 可选：字段映射配置
  # schema = {
  #   fields {
  #     # 字段定义将根据实际查询结果自动推断
  #   }
  # }
}
```

### 2. Sqoop MySQL JDBC Source模板 (sqoop/sources/mysql-jdbc-source.conf)
```hocon
# Sqoop MySQL JDBC Source连接器模板
# 使用Sqoop专用的占位符语法从Sqoop配置中提取数据
Jdbc {
  # 数据库连接配置 - Sqoop专用路径
  url = "${sqoop:connection.url}"
  driver = "com.mysql.cj.jdbc.Driver"
  user = "${sqoop:connection.username}"
  password = "${sqoop:connection.password|}"
  
  # 查询配置 - Sqoop的表和查询配置
  query = "${sqoop:query|SELECT ${sqoop:columns|*} FROM ${sqoop:table}}"
  
  # 数据分割配置（可选）- Sqoop专用参数
  partition_column = "${sqoop:split.by|}"
  partition_num = ${sqoop:num.mappers|1}
  
  # 连接池配置
  connection_check_timeout_sec = 60
  
  # 结果表名
  result_table_name = "source_table"
}
```

### 3. DataX Hive Sink模板 (datax/sinks/hive-sink.conf)
```hocon
# DataX Hive Sink连接器模板
Hive {
  # Hive连接配置 - DataX专用路径
  metastore_uri = "${datax:job.content[0].writer.parameter.metastoreUris|thrift://localhost:9083}"
  
  # 表配置 - DataX专用参数
  database = "${datax:job.content[0].writer.parameter.database|default}"
  table_name = "${datax:job.content[0].writer.parameter.fileName}"
  
  # 文件格式配置
  file_format = "${datax:job.content[0].writer.parameter.fileType|@file_type_mapper}"
  
  # 存储路径配置
  path = "${datax:job.content[0].writer.parameter.path}"
  
  # 分区配置（如果DataX配置中有分区信息）
  partition_by = [${datax:job.content[0].writer.parameter.partition|}]
  
  # 压缩配置
  compress_codec = "${datax:job.content[0].writer.parameter.compress|@compress_mapper}"
  
  # 写入模式配置
  save_mode = "${datax:job.content[0].writer.parameter.writeMode|@write_mode_mapper}"
  
  # Hive配置参数
  hive_conf = {
    # 动态分区配置
    "hive.exec.dynamic.partition" = "true"
    "hive.exec.dynamic.partition.mode" = "nonstrict"
    
    # 文件合并配置
    "hive.merge.mapfiles" = "true"
    "hive.merge.mapredfiles" = "true"
    "hive.merge.size.per.task" = "256000000"
    "hive.merge.smallfiles.avgsize" = "128000000"
  }
  
  # 可选：自定义Hadoop配置
  hadoop_conf = {
    "fs.defaultFS" = "${datax:job.content[0].writer.parameter.defaultFS|hdfs://localhost:9000}"
    # 其他Hadoop配置可以在这里添加
  }
  
  # 可选：表属性配置
  table_properties = {
    # 表的存储格式属性
    "serialization.format" = "1"
    
    # ORC格式特定配置（如果使用ORC）
    "orc.compress" = "${datax:job.content[0].writer.parameter.compress|SNAPPY}"
    "orc.stripe.size" = "268435456"
    "orc.row.index.stride" = "10000"
    
    # Parquet格式特定配置（如果使用Parquet）
    "parquet.compression" = "${datax:job.content[0].writer.parameter.compress|SNAPPY}"
    "parquet.block.size" = "268435456"
    "parquet.page.size" = "1048576"
  }
}
```

### 4. DataX 环境配置模板 (datax/env/batch-env.conf)
```hocon
# DataX 批处理环境配置模板
env {
  # 并行度配置：从DataX的channel数量获取，默认为1
  parallelism = ${datax:job.setting.speed.channel|1}
  
  # 任务模式：批处理模式
  job.mode = "BATCH"
  
  # 检查点配置
  checkpoint.interval = ${datax:job.setting.speed.channel|10000}
  
  # 任务名称
  job.name = "DataX2SeaTunnel_${datax:job.content[0].reader.name}_to_${datax:job.content[0].writer.name}"
  
  # 任务描述
  job.description = "Convert DataX ${datax:job.content[0].reader.name} to SeaTunnel ${datax:job.content[0].writer.name}"
  
  # 任务标签
  job.tags = ["datax", "conversion", "batch"]
}
```

### 5. Sqoop 环境配置模板 (sqoop/env/import-env.conf)
```hocon
# Sqoop 导入环境配置模板
env {
  # 并行度配置：从Sqoop的mappers数量获取，默认为1
  parallelism = ${sqoop:num.mappers|1}
  
  # 任务模式：批处理模式
  job.mode = "BATCH"
  
  # 检查点配置
  checkpoint.interval = 10000
  
  # 任务名称
  job.name = "Sqoop2SeaTunnel_${sqoop:table}_import"
  
  # 任务描述
  job.description = "Convert Sqoop import of ${sqoop:table} to SeaTunnel"
  
  # 任务标签
  job.tags = ["sqoop", "import", "conversion", "batch"]
}
```
```hocon
# Hive Sink连接器模板
Hive {
  # Hive连接配置
  metastore_uri = "${datax:job.content[0].writer.parameter.metastoreUris|thrift://localhost:9083}"
  
  # 表配置
  database = "${datax:job.content[0].writer.parameter.database|default}"
  table_name = "${datax:job.content[0].writer.parameter.fileName}"
  
  # 文件格式配置
  file_format = "${datax:job.content[0].writer.parameter.fileType|@file_type_mapper}"
  
  # 路径配置
  path = "${datax:job.content[0].writer.parameter.path}"
  
  # 分区配置（如果有）
  partition_by = [${datax:job.content[0].writer.parameter.partition|}]
  
  # 压缩配置
  compress_codec = "${datax:job.content[0].writer.parameter.compress|@compress_mapper}"
  
  # 写入模式
  save_mode = "${datax:job.content[0].writer.parameter.writeMode|@write_mode_mapper}"
  
  # Hive配置参数
  hive_conf = {
    # 动态分区配置
    "hive.exec.dynamic.partition" = "true"
    "hive.exec.dynamic.partition.mode" = "nonstrict"
    
    # 文件合并配置
    "hive.merge.mapfiles" = "true"
    "hive.merge.mapredfiles" = "true"
    "hive.merge.size.per.task" = "256000000"
    "hive.merge.smallfiles.avgsize" = "128000000"
  }
  
  # 可选：自定义Hadoop配置
  hadoop_conf = {
    "fs.defaultFS" = "${datax:job.content[0].writer.parameter.defaultFS|hdfs://localhost:9000}"
  }
  
  # 可选：表属性配置
  table_properties = {
    "serialization.format" = "1"
    "orc.compress" = "${datax:job.content[0].writer.parameter.compress|SNAPPY}"
    "parquet.compression" = "${datax:job.content[0].writer.parameter.compress|SNAPPY}"
  }
}
```

### 3. 批处理环境配置模板 (env/batch-env.conf)
```hocon
# 批处理环境配置模板
env {
  # 并行度配置
  parallelism = ${datax:job.setting.speed.channel|1}
  
  # 任务模式
  job.mode = "BATCH"
  
  # 检查点配置
  checkpoint.interval = ${datax:job.setting.speed.channel|10000}
  
  # 任务名称
  job.name = "DataX2SeaTunnel_${datax:job.content[0].reader.name}_to_${datax:job.content[0].writer.name}"
  
  # 其他环境配置
  # job.retry.times = 3
  # job.retry.interval = "10s"
}
```

### 4. PostgreSQL JDBC Source模板 (sources/postgresql-jdbc-source.conf)
```hocon
# PostgreSQL JDBC Source连接器模板
Jdbc {
  # 数据库连接配置
  url = "${datax:job.content[0].reader.parameter.connection[0].jdbcUrl[0]}"
  driver = "org.postgresql.Driver"
  user = "${datax:job.content[0].reader.parameter.username}"
  password = "${datax:job.content[0].reader.parameter.password|}"
  
  # 查询配置
  query = "${datax:job.content[0].reader.parameter.querySql[0]|SELECT ${datax:job.content[0].reader.parameter.column[*]|*} FROM ${datax:job.content[0].reader.parameter.connection[0].table[0]}}"
  
  # 数据分割配置（可选）
  partition_column = "${datax:job.content[0].reader.parameter.splitPk|}"
  partition_num = ${datax:job.setting.speed.channel|1}
  
  # 连接池配置
  connection_check_timeout_sec = 60
  
  # 结果表名
  result_table_name = "source_table"
  
  # PostgreSQL特定配置
  connection_properties = {
    "applicationName" = "SeaTunnel_X2_Conversion"
    "loginTimeout" = "30"
    "socketTimeout" = "60"
    "tcpKeepAlive" = "true"
    "ssl" = "${datax:job.content[0].reader.parameter.ssl|false}"
    "sslmode" = "${datax:job.content[0].reader.parameter.sslmode|disable}"
  }
}
```

### 5. HDFS Sink模板 (sinks/hdfs-sink.conf)
```hocon
# HDFS Sink连接器模板
HDFS {
  # HDFS路径配置
  path = "${datax:job.content[0].writer.parameter.path}"
  default_fs = "${datax:job.content[0].writer.parameter.defaultFS|hdfs://localhost:9000}"
  
  # 文件配置
  file_name_expression = "${datax:job.content[0].writer.parameter.fileName|part-${uuid()}}"
  file_format = "${datax:job.content[0].writer.parameter.fileType|@file_type_mapper}"
  
  # 字段分隔符（文本格式时使用）
  field_delimiter = "${datax:job.content[0].writer.parameter.fieldDelimiter|,}"
  
  # 行分隔符（文本格式时使用）
  row_delimiter = "${datax:job.content[0].writer.parameter.rowDelimiter|\n}"
  
  # 压缩配置
  compress_codec = "${datax:job.content[0].writer.parameter.compress|@compress_mapper}"
  
  # 写入模式
  save_mode = "${datax:job.content[0].writer.parameter.writeMode|@write_mode_mapper}"
  
  # 文件大小配置
  max_file_size = "${datax:job.content[0].writer.parameter.maxFileSize|134217728}"  # 128MB
  
  # Hadoop配置
  hadoop_config = {
    "fs.defaultFS" = "${datax:job.content[0].writer.parameter.defaultFS|hdfs://localhost:9000}"
    "dfs.replication" = "${datax:job.content[0].writer.parameter.replication|3}"
    "dfs.block.size" = "${datax:job.content[0].writer.parameter.blockSize|134217728}"
  }
  
  # 特定文件格式配置
  format_options = {
    # Parquet格式配置
    "parquet.block.size" = "${datax:job.content[0].writer.parameter.blockSize|134217728}"
    "parquet.page.size" = "${datax:job.content[0].writer.parameter.pageSize|1048576}"
    "parquet.compression" = "${datax:job.content[0].writer.parameter.compress|SNAPPY}"
    
    # ORC格式配置
    "orc.stripe.size" = "${datax:job.content[0].writer.parameter.stripeSize|268435456}"
    "orc.compress" = "${datax:job.content[0].writer.parameter.compress|SNAPPY}"
    "orc.row.index.stride" = "${datax:job.content[0].writer.parameter.rowIndexStride|10000}"
    
    # 文本格式配置
    "text.encoding" = "${datax:job.content[0].writer.parameter.encoding|UTF-8}"
    "text.null.format" = "${datax:job.content[0].writer.parameter.nullFormat|\\N}"
  }
}
```

## 转换报告设计

### 报告格式示例
```markdown
# DataX到SeaTunnel转换报告

## 基本信息
- **源文件**: `datax-mysql2hive.json`
- **使用模板**: 
  - Source: `sources/mysql-jdbc-source.conf`
  - Sink: `sinks/hive-sink.conf`
  - Environment: `env/batch-env.conf`
- **转换时间**: `2025-07-04 16:30:45`
- **转换状态**: `成功`

## 占位符替换详情

### ✅ 成功替换 (12个)
- `${datax:job.content[0].reader.parameter.connection[0].jdbcUrl[0]}` → `jdbc:mysql://localhost:3306/test`
- `${datax:job.content[0].reader.parameter.username}` → `root`
- `${datax:job.content[0].reader.parameter.password|}` → `""` (使用默认值)
- `${datax:job.content[0].writer.parameter.fileName}` → `target_table`
- `${datax:job.content[0].writer.parameter.database}` → `warehouse`
- `${datax:job.content[0].writer.parameter.path}` → `/user/hive/warehouse/test.db/target_table`
- `${datax:job.content[0].writer.parameter.fileType|@file_type_mapper}` → `orc` (通过转换器)
- `${datax:job.setting.speed.channel}` → `3`
- `${datax:job.content[0].reader.name}` → `mysqlreader`
- `${datax:job.content[0].writer.name}` → `hivewriter`
- `${datax:job.content[0].reader.parameter.column[*]}` → `id, name, age, email`
- `${datax:job.content[0].reader.parameter.connection[0].table[0]}` → `users`

### 🔧 转换器应用 (2个)
- `file_type_mapper`: `orc` → `orc`
- `compress_mapper`: `snappy` → `snappy`

### ⚠️ 使用默认值 (3个)
- `metastore_uri`: 使用默认值 `thrift://localhost:9083`
- `compress_codec`: 使用默认值 `none`
- `save_mode`: 使用默认值 `append`

### ❌ 占位符错误 (0个)
*无占位符处理错误*

## 配置验证结果

### ✅ 验证通过项目
- HOCON语法验证: 通过
- 必填字段验证: 通过  
- URL格式验证: 通过
- 字段类型验证: 通过

### ⚠️ 验证警告 (1个)
- 密码字段为空，建议在生产环境中设置

## 生成的配置预览
```hocon
env {
  parallelism = 3
  job.mode = "BATCH"
  checkpoint.interval = 10000
  job.name = "DataX2SeaTunnel_mysqlreader_to_hivewriter"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = ""
    query = "SELECT id, name, age, email FROM users"
    result_table_name = "source_table"
  }
}

sink {
  Hive {
    metastore_uri = "thrift://localhost:9083"
    database = "warehouse"
    table_name = "target_table"
    file_format = "orc"
    path = "/user/hive/warehouse/test.db/target_table"
    compress_codec = "snappy"
    save_mode = "append"
  }
}
```

## 建议
- ✅ 配置转换成功，可以直接使用
- ⚠️ 建议设置数据库密码
- 💡 建议验证目标Hive表的schema是否匹配
```

## 实现计划

### 迭代1.2：多工具支持的模板引擎 (1.5周)
**目标**: 实现支持多工具的基础模板引擎

**主要任务**:
1. 实现 `ToolIdentifier` - 工具类型识别器
2. 实现 `TemplateMappingResolver` - 多工具模板选择器
3. 实现 `PlaceholderProcessor` - 支持多工具占位符处理器
4. 实现 `TemplateAssembler` - 模板组装器
5. 创建DataX的MySQL→Hive、MySQL→HDFS模板文件
6. 实现配置验证器
7. 编写单元测试

**验证标准**:
```bash
# 使用DataX的MySQL到Hive模板进行转换
./bin/x2seatunnel.sh -t datax -s examples/datax-mysql2hive.json -o output/mysql2hive.conf

# 验证生成的配置文件包含正确的占位符替换结果
```

### 迭代1.3：完整DataX模板库 (1周)  
**目标**: 完善DataX模板库和高级特性

**主要任务**:
1. 创建更多DataX模板文件 (PostgreSQL→Hive, Oracle→HDFS等)
2. 实现高级转换器 (值映射、条件处理等)
3. 完善配置验证规则
4. 实现嵌套占位符处理
5. 优化错误处理和报告生成
6. 编写端到端测试

**验证标准**:
```bash
# 测试多种DataX连接器组合
./bin/x2seatunnel.sh -t datax -s examples/datax-mysql2hdfs.json -o output/mysql2hdfs.conf
./bin/x2seatunnel.sh -t datax -s examples/datax-postgresql2hive.json -o output/postgresql2hive.conf

# 验证转换报告的完整性和准确性
```

### 迭代1.4：Sqoop工具支持 (1.5周)
**目标**: 扩展支持Sqoop工具

**主要任务**:
1. 实现Sqoop配置解析器
2. 创建Sqoop专用的占位符处理逻辑
3. 创建Sqoop模板文件库
4. 实现Sqoop特殊配置转换
5. 完善多工具转换报告
6. 编写Sqoop转换测试

**验证标准**:
```bash
# 测试Sqoop转换
./bin/x2seatunnel.sh -t sqoop -s examples/sqoop-mysql2hive.properties -o output/sqoop-mysql2hive.conf

# 验证Sqoop和DataX工具的隔离性
```

### 迭代1.5：性能优化和扩展 (0.5周)
**目标**: 优化性能和完善功能

**主要任务**:
1. 实现模板热更新机制
2. 优化模板缓存和性能
3. 完善文档和示例
4. 实现批量转换功能
5. 添加更多连接器模板

## 优势总结

### 1. **多工具支持优势**
- **工具隔离**: 每个工具使用独立的模板和占位符语法，完全隔离
- **专业化**: 每个工具可以充分利用其特有的配置参数
- **无干扰**: 不同工具的扩展不会相互影响
- **易扩展**: 新增工具支持只需创建对应的模板目录

### 2. **架构设计优势**
- **模板数量大幅优化**: 从组合爆炸减少到线性增长
- **灵活组合**: 任意Source和Sink可以自由组合
- **组件独立**: 每个模板独立维护，互不影响
- **配置完整**: 确保生成的SeaTunnel配置包含所有必要字段

### 3. **用户体验优势**
- **直观易懂**: 直接使用SeaTunnel原生配置格式
- **学习成本低**: 无需学习额外的映射语法
- **配置预览**: 用户能直接看到最终的配置效果
- **错误友好**: 详细的转换报告和验证结果

### 4. **开发维护优势**
- **零代码扩展**: 所有扩展都通过配置文件实现
- **热更新**: 修改模板文件立即生效
- **版本控制**: 每个模板独立版本管理
- **测试独立**: 每个工具的测试可以独立进行

### 5. **技术实现优势**
- **占位符语法专用**: 每个工具使用最适合的占位符语法
- **高兼容性**: 支持DataX、Sqoop、Flume等多种工具
- **强可扩展性**: 水平扩展（新连接器）和垂直扩展（新工具）都很简单
- **低复杂度**: 模板选择和组装都是简单的字符串操作

这种基于多工具支持和Source/Sink分离的设计方案将大大简化用户的使用体验，同时保持强大的扩展能力和配置完整性保证，为后续支持更多数据同步工具奠定了坚实的基础。
