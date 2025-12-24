# 常见问题解答

## SeaTunnel 支持哪些数据来源和数据目的地？
SeaTunnel 支持多种数据源来源和数据目的地，您可以在官网找到详细的列表：
SeaTunnel 支持的数据来源(Source)列表：https://seatunnel.apache.org/docs/connector-v2/source
SeaTunnel 支持的数据目的地(Sink)列表：https://seatunnel.apache.org/docs/connector-v2/sink

## SeaTunnel 是否支持批处理和流处理？
SeaTunnel 支持批流一体，SeaTunnel 可以设置批处理和流处理两种模式。您可以根据具体的业务场景和需求选择合适的处理模式。批处理适合定时数据同步场景，而流处理适合实时同步和数据变更捕获 (CDC) 场景。

## 使用 SeaTunnel 需要安装 Spark 或者 Flink 这样的引擎么？
Spark 和 Flink 不是必需的，SeaTunnel 可以支持 Zeta、Spark 和 Flink 3 种作为同步引擎的选择，您可以选择之一就行，社区尤其推荐使用 Zeta 这种专为同步场景打造的新一代超高性能同步引擎。Zeta 被社区用户亲切的称为 “泽塔奥特曼”!
社区对 Zeta 的支持力度是最大的，功能也更丰富。

## SeaTunnel 支持的数据转换功能有哪些？
SeaTunnel 支持多种数据转换功能，包括字段映射、数据过滤、数据格式转换等。可以通过在配置文件中定义 `transform` 模块来实现数据转换。详情请参考 SeaTunnel [Transform 文档](https://seatunnel.apache.org/docs/transform-v2)。

## SeaTunnel 是否可以自定义数据清洗规则？
SeaTunnel 支持自定义数据清洗规则。可以在 `transform` 模块中配置自定义规则，例如清理脏数据、删除无效记录或字段转换。

## SeaTunnel 是否支持实时增量同步？
SeaTunnel 支持增量数据同步。例如通过 CDC 连接器实现对数据库的增量同步，适用于需要实时捕获数据变更的场景。

## SeaTunnel 目前支持哪些数据源的 CDC ？
目前支持 MongoDB CDC、MySQL CDC、Opengauss CDC、Oracle CDC、PostgreSQL CDC、Sql Server CDC、TiDB CDC等，更多请查阅[Source](https://seatunnel.apache.org/docs/connector-v2/source)。

## SeaTunnel CDC 同步需要的权限如何开启？
这样就可以了。
这里多说一句，连接器对应的 cdc 权限开启步骤在官网都有写，请参照 SeaTunnel 对应的官网操作即可

## SeaTunnel 支持从 MySQL 备库进行 CDC 么？日志如何拉取？
支持，是通过订阅 MySQL binlog 日志方式到同步服务器上解析 binlog 日志方式进行

## SeaTunnel 是否支持无主键表的 CDC 同步？
不支持无主键表的 cdc 同步。原因如下：
比如上游有 2 条一模一样的数据，然后上游删除或修改了一条，下游由于无法区分到底是哪条需要删除或修改，会出现这 2 条都被删除或修改的情况。
没主键要类似去重的效果本身有点儿自相矛盾，就像辨别西游记里的真假悟空，到底哪个是真的

## SeaTunnel 是否支持自动建表？
在同步任务启动之前，可以为目标端已有的表结构选择不同的处理方案。是通过 `schema_save_mode` 参数来控制的。
`schema_save_mode` 有以下几种方式可选：
- **`RECREATE_SCHEMA`**：当表不存在时会创建，若表已存在则删除并重新创建。
- **`CREATE_SCHEMA_WHEN_NOT_EXIST`**：当表不存在时会创建，若表已存在则跳过创建。
- **`ERROR_WHEN_SCHEMA_NOT_EXIST`**：当表不存在时会报错。
- **`IGNORE`**：忽略对表的处理。
  目前很多 connector 已经支持了自动建表，请参考对应的 connector 文档，这里拿 Jdbc 举例，请参考 [Jdbc sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc#schema_save_mode-enum)

## SeaTunnel 是否支持数据同步任务开始前对已有数据进行处理？
在同步任务启动之前，可以为目标端已有的数据选择不同的处理方案。是通过 `data_save_mode` 参数来控制的。
`data_save_mode` 有以下几种可选项：
- **`DROP_DATA`**：保留数据库结构，删除数据。
- **`APPEND_DATA`**：保留数据库结构，保留数据。
- **`CUSTOM_PROCESSING`**：用户自定义处理。
- **`ERROR_WHEN_DATA_EXISTS`**：当存在数据时，报错。
  目前很多 connector 已经支持了对已有数据进行处理，请参考对应的 connector 文档，这里拿 Jdbc 举例，请参考 [Jdbc sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc#data_save_mode-enum)

## SeaTunnel 是否支持精确一致性管理？
SeaTunnel 支持一部分数据源的精确一致性，例如支持 MySQL、PostgreSQL 等数据库的事务写入，确保数据在同步过程中的一致性，另外精确一致性也要看数据库本身是否可以支持

## SeaTunnel 可以定期执行任务吗？
您可以通过使用 linux 自带 cron 能力来实现定时数据同步任务，也可以结合 DolphinScheduler 等调度工具实现复杂的定时任务管理。

## 我有一个问题，我自己无法解决
我在使用 SeaTunnel 时遇到了问题，无法自行解决。 我应该怎么办？有以下几种方式
1、在[问题列表](https://github.com/apache/seatunnel/issues)或[邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)中搜索看看是否有人已经问过同样的问题并得到答案。
2、如果您找不到问题的答案，您可以通过[这些方式](https://github.com/apache/seatunnel#contact-us)联系社区成员寻求帮助。
3、中国用户可以添加微信群助手：seatunnel1，加入社区交流群，也欢迎大家关注微信公众号：seatunnel。

## 如何声明变量？
您想知道如何在 SeaTunnel 的配置中声明一个变量，然后在运行时动态替换该变量的值吗？ 该功能常用于定时或非定时离线处理，以替代时间、日期等变量。 用法如下：
在配置中配置变量名称。 下面是一个sql转换的例子（实际上，配置文件中任何地方“key = value”中的值都可以使用变量替换）：
```
...
transform {
  Sql {
    query = "select * from dual where city ='${city}' and dt = '${date}'"
  }
}
...
```

以使用 SeaTunnel Zeta Local模式为例，启动命令如下：

```bash
$SEATUNNEL_HOME/bin/seatunnel.sh \
-c $SEATUNNEL_HOME/config/your_app.conf \
-m local[2] \
-i city=Singapore \
-i date=20231110
```

您可以使用参数“-i”或“--variable”后跟“key=value”来指定变量的值，其中key需要与配置中的变量名称相同。详情可以参考：https://seatunnel.apache.org/docs/concept/config

## 如何在配置文件中写入多行文本的配置项？
当配置的文本很长并且想要将其换行时，您可以使用三个双引号来指示其开始和结束：

```
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
```

## 如何实现多行文本的变量替换？
在多行文本中进行变量替换有点麻烦，因为变量不能包含在三个双引号中：

```
var = """
your string 1
"""${you_var}""" your string 2"""
```

请参阅：[lightbend/config#456](https://github.com/lightbend/config/issues/456)。


## 如果想学习 SeaTunnel 的源代码，应该从哪里开始？
SeaTunnel 拥有完全抽象、结构化的非常优秀的架构设计和代码实现，很多用户都选择 SeaTunnel 作为学习大数据架构的方式。 您可以从`seatunnel-examples`模块开始了解和调试源代码：SeaTunnelEngineLocalExample.java
具体参考：https://seatunnel.apache.org/docs/contribution/setup
针对中国用户，如果有伙伴想贡献自己的一份力量让 SeaTunnel 更好，特别欢迎加入社区贡献者种子群，欢迎添加微信：davidzollo，添加时请注明 "参与开源共建", 群仅仅用于技术交流, 重要的事情讨论还请发到 dev@seatunnel.apache.org 邮件里进行讨论。

## 如果想开发自己的 source、sink、transform 时，是否需要了解 SeaTunnel 所有源代码？
不需要，您只需要关注 source、sink、transform 对应的接口即可。
如果你想针对 SeaTunnel API 开发自己的连接器（Connector V2），请查看**[Connector Development Guide](https://github.com/apache/seatunnel/blob/dev/seatunnel-connectors-v2/README.zh.md)** 。


