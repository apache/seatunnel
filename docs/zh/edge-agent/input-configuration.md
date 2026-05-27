---
sidebar_position: 6
title: 输入配置
---

# 文件输入配置指南

在 agent.yaml 的 input 段配置文件采集。参数类型与默认值仅以[配置说明 — input](configuration.md#input) 为准；本文提供 路径 glob、单行与多行逻辑事件，以及 可直接套用的 YAML。

Edge Agent 的 input 段配置 file 采集器（type: file，默认）：尾随本地文件、支持多行合并、将每条逻辑事件序列化后写入出站队列。

## 路径与文件发现

### paths

paths 为 glob 模式或具体文件路径的列表。采集器会：

- 启动时解析所有模式，打开匹配的普通文件（不含目录本身）；
- 按 glob-scan-interval-ms（默认 5000）周期性重新 glob，发现新文件（例如日志轮转生成 app.log.2025-05-19）；
- 首次发现时按文件最后修改时间升序处理（先旧后新）。

:::note

模式使用 Java NIO glob: 语法（*、**、?、[...]）。Windows 上也请使用正斜杠 /。

:::


| 模式示例                        | 匹配范围                             |
| --------------------------- | -------------------------------- |
| /var/log/myapp/*.log      | /var/log/myapp/ 下所有 .log     |
| /var/log/myapp/**         | 该目录树下所有普通文件（递归）                  |
| /var/log/nginx/access.log | 单个文件                             |
| /data/events/app-*.json   | 如 app-001.json、app-prod.json |


路径中无 glob 字符且指向目录时，会采集该目录下所有普通文件（等价于追加 **/*）。

### 尾随 vs 从头读


| 配置项                 | 值      | 典型场景                                    |
| ------------------- | ------ | --------------------------------------- |
| read-from-beginning | false（默认） | 生产尾随：首次打开且无已存位点时，从文件末尾开始（只读新增内容）。   |
| read-from-beginning | true      | 补数或一次性追历史：首次打开从字节 0 读。重启后仍以已保存位点为准。 |


:::note

位点保存在与出站队列相同的 WAL 数据库中，重启后从上次持久化偏移继续（与 WAL append 同步，不以 Engine ACK 为准）。

:::

### 轮转与多文件


| 配置项                     | 作用                                |
| ----------------------- | --------------------------------- |
| glob-scan-interval-ms | 重新 glob 并挂载新路径的间隔。            |
| close-inactive-ms     | 超过该时间无读取则关闭文件句柄（默认 5 分钟）。         |
| on-error: skip        | 单文件 IO 错误时跳过继续（fail 则终止 Agent）。 |


---

## 单行事件 vs 多行合并

默认（不写 multiline 或未写 pattern）： 每个物理行一个事件。适用于 NDJSON（每行一个 JSON）或很少堆栈的文本日志。

多行（设置 multiline.pattern）： 按正则判断事件边界，将多行缓冲为一条逻辑事件。


| multiline.match | 含义                                                       |
| ----------------- | -------------------------------------------------------- |
| after（默认）       | 匹配正则的行作为新事件起始，此前缓冲行作为上一条事件发出。常见于每行以时间戳开头的日志。 |
| before          | 匹配正则的行作为当前事件的最后一行，追加后立即 flush。                   |
| negate: true    | 对匹配结果取反（高级用法）。                                           |



| 配置项         | 作用                       |
| ----------- | ------------------------ |
| max-lines | 单条事件最多缓冲的物理行数（默认 500）。 |
| flush-idle-timeout-ms | 缓冲区空闲超过该毫秒数时强制 flush（默认 5000）。必须 > 0。 |


:::note

YAML 双引号字符串中，正则反斜杠需转义（例如 `"^\\d{4}-\\d{2}-\\d{2}"`）。

:::

### 序列化形态


| output-format.type | 下游收到                                                        |
| -------------------- | ----------------------------------------------------------- |
| line（默认）           | 每条事件一个 JSON，含 _file、_line、_offset、payload（单行或多行数组）。 |
| json               | 在可解析时输出结构化 JSON。                                            |


此为 Agent 事件封装，不是 EdgeSocket 线路编码，见 [EdgeSocket Source](../connectors/source/EdgeSocket.md)。

---

## 场景配置示例

以下仅展示 input；output、queue 等见 [部署指南](deployment-guide.md)。

### 1. 单应用目录 — 通配符采集

采集某目录下全部 *.log，只尾随新写入。

```yaml
input:
  paths:
    - "/var/log/myapp/*.log"
  read-from-beginning: false
  glob-scan-interval-ms: 5000
```

### 2. 多目录或多命名模式

Nginx 访问/错误日志 + JSON 事件目录：

```yaml
input:
  paths:
    - "/var/log/nginx/access.log"
    - "/var/log/nginx/error.log"
    - "/data/myapp/events/**/*.json"
  encoding: UTF-8
```

### 3. NDJSON

不配置 multiline，每物理行一条事件，payload 为原始行内容。

```yaml
input:
  paths:
    - "/data/ingest/*.ndjson"
  output-format:
    type: line
```

文件示例：

```text
{"user":"a","action":"login"}
{"user":"b","action":"logout"}
```

### 4. Java / Spring 日志 — 时间戳开启新事件

以 2025-05-19 开头的行作为新事件起点，堆栈行归属上一条。

```yaml
input:
  paths:
    - "/var/log/spring-boot/application.log"
  multiline:
    pattern: "^\\d{4}-\\d{2}-\\d{2}"
    match: after
    max-lines: 500
    flush-idle-timeout-ms: 5000
  output-format:
    type: line
```

磁盘示例：

```text
2025-05-19 10:00:00 ERROR com.example.Main - failed
java.lang.RuntimeException: boom
    at com.example.Main.run(Main.java:1)
2025-05-19 10:00:01 INFO  com.example.Main - recovered
```

→ 两条事件：第一条 3 行；第二条 1 行。

### 5. 以结束标记收束

固定尾行（如 ---END---）表示一条记录结束。

```yaml
input:
  paths:
    - "/opt/batch/output/*.txt"
  multiline:
    pattern: "^---END---$"
    match: before
```

### 6. 首次安装补历史，之后尾随

首次部署从文件头读取；重启后按已存位点续读。

```yaml
input:
  paths:
    - "/var/log/myapp/*.log"
  read-from-beginning: true
```

:::tip

首次跑完后可改回 read-from-beginning: false，重启仍以位点为准。

:::

### 7. 按日期滚动的文件

如 app-2025-05-19.log 每日新增；glob + glob-scan-interval-ms 自动发现新文件。

```yaml
input:
  paths:
    - "/var/log/myapp/app-*.log"
  glob-scan-interval-ms: 3000
  close-inactive-ms: 600000
```

### 8. 固定 input.id

省略 id 时使用稳定的采集源标识作为 WAL / 位点 sourceId（见 [身份文件](configuration.md#身份文件edge-agentid)）。多 Agent 或 WAL 迁移时请保留 edge-agent.id 与 WAL，或显式指定：

```yaml
input:
  id: "edge-host-01-nginx"
  paths:
    - "/var/log/nginx/*.log"
```

---

## 嵌套 input.file

可将与 input 平级相同的键写在 input.file 下。两者同时存在时，file 块覆盖同名顶层字段。无特殊需求时建议用扁平 input。

```yaml
input:
  id: my-source
  file:
    paths:
      - "/var/log/*.log"
    multiline:
      pattern: "^\\["
      match: after
```

---

