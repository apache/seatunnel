## 目的

SeaTunnel为与计算引擎进行解耦，设计了新的连接器API，通过这篇文章来介绍新的接口以及新的代码结构，方便开发者快速上手使用新版API开发连接器并理解新版API运行原理.
详细设计请查看该[提议](https://github.com/apache/seatunnel/issues/1608) 。

## 代码结构

为了和老的代码分开，方便现阶段的并行开发，以及降低merge的难度。我们为新的执行流程定义了新的模块

### **工程结构**

- ../`seatunnel-connectors-v2`                                         connector-v2代码实现
- ../`seatunnel-translation`                                           connector-v2的翻译层
- ../`seatunnel-transform-v2`                                          transform-v2代码实现
- ../seatunnel-e2e/`seatunnel-connector-v2-e2e`                        connector-v2端到端测试
- ../seatunnel-examples/`seatunnel-engine-examples`                    seatunnel connector-v2的Zeta引擎local运行的实例
- ../seatunnel-examples/`seatunnel-flink-connector-v2-example`         seatunnel connector-v2的flink local运行的实例
- ../seatunnel-examples/`seatunnel-spark-connector-v2-example`         seatunnel connector-v2的spark local运行的实例

### Example

我们已经在`seatunnel-examples`准备了三个本地可执行的案例程序
- `seatunnel-examples/seatunnel-engine-examples/src/main/java/org/apache/seatunnel/example/engine/SeaTunnelEngineLocalExample.java`，它运行在Zeta引擎上
- `seatunnel-examples/seatunnel-flink-connector-v2-example/src/main/java/org/apache/seatunnel/example/flink/v2/SeaTunnelApiExample.java`，它运行在flink引擎上
- `seatunnel-examples/seatunnel-spark-connector-v2-example/src/main/java/org/apache/seatunnel/example/spark/v2/SeaTunnelApiExample.java`，它运行在spark引擎上
你可以通过调试这些例子帮你更好的理解程序运行逻辑。使用的配置文件保存在`resources/examples`文件夹里。如果你想增加自己的connectors，你需要按照下面的步骤。

以Zeta引擎为例，你可以通过以下步骤来添加一个新的connector到example中:
1. 在`seatunnel-examples/seatunnel-engine-examples/pom.xml`添加connector依赖的groupId, artifactId 和
   version.（或者当你想在flink或spark引擎运行时请在`seatunnel-examples/seatunnel-flink-connector-v2-example/pom.xml`或`seatunnel-examples/seatunnel-spark-connector-v2-example/pom.xml`添加依赖，以下同理）。
2. 如果你的connector中存在scope为test或provided的依赖，将这些依赖添加到`seatunnel-examples/seatunnel-engine-examples/pom.xml`并且修改scope为compile。
3. 在resources/examples下添加任务配置文件。
4. 在`SeaTunnelEngineLocalExample.java` main方法中配置文件地址。
5. 运行main方法即可。

### 创建新的SeaTunnel V2 Connector

1.在`seatunnel-connectors-v2`目录下新建一个module，命名为connector-{连接器名}.

2.pom文件可以参考已有连接器的pom文件，并在`seatunnel-connectors-v2/pom.xml`中添加当前子model.

3.新建两个package分别对应source和sink

    package org.apache.seatunnel.connectors.seatunnel.{连接器名}.source
    package org.apache.seatunnel.connectors.seatunnel.{连接器名}.sink

4.将连接器信息添加到在项目根目录的plugin-mapping.properties文件中.

5.将连接器添加到seatunnel-dist/pom.xml,这样连接器jar就可以在二进制包中找到.

6.source端有几个必须实现的类，分别是{连接器名}Source、{连接器名}SourceFactory、{连接器名}SourceReader；sink端有几个必须实现的类，分别是{连接器名}Sink、{连接器名}SinkFactory、{连接器名}SinkWriter，具体可以参考其他连接器

7.{连接器名}SourceFactory 和 {连接器名}SinkFactory 里面需要在类名上标注 **@AutoService(Factory.class)** 注解，并且除了必须实现的方法外，source端需要额外再重写一个 **createSource** 方法，sink端需要额外再重写一个 **createSink** 方法

8.{连接器名}Source 需要重写 **getProducedCatalogTables** 方法；{连接器名}Sink 需要重写 **getWriteCatalogTable** 方法

### 启动类

我们创建了三个启动类工程，分别是`seatunnel-core/seatunnel-starter`，`seatunnel-core/seatunnel-flink-starter`和`seatunnel-core/seatunnel-spark-starter`。
可以在这里找到如何将配置文件解析为可以执行的Zeta/Flink/Spark流程。

### SeaTunnel API

`seatunnel-api`模块，用于存放SeaTunnel API定义的新接口, 开发者通过对这些接口进行实现，就可以完成支持多引擎的SeaTunnel Connector

### 翻译层

我们通过适配不同引擎的接口，实现SeaTunnel API和Engine API的转换，从而达到翻译的效果，让我们的SeaTunnel Connector支持多个不同引擎的运行。 对应代码地址为`seatunnel-translation`
,该模块有对应的翻译层实现。感兴趣可以查看代码，帮助我们完善当前代码。

## API 介绍

`SeaTunnel 当前版本的API设计借鉴了Flink的设计理念`

### Source

#### TableSourceFactory.java

- 用于创建Source的工厂类，通过该类来创建Source实例，通过`createSource`方法来创建Source实例。
- `factoryIdentifier`用于标识当前Factory的名称，也是在配置文件中配置的名称，用于区分不同的连接器。
- `optionRule` 用于定义当前连接器支持的参数，可以通过该方法来定义参数的逻辑，比如哪些参数是必须的，哪些参数是可选的，哪些参数是互斥的等等，SeaTunnel会通过OptionRule来校验用户的配置是否合法。请参考下方的Option。
- 请确保在`TableSourceFactory`添加`@AutoService(Factory.class)`注解。

#### SeaTunnelSource.java

- SeaTunnel的Source采用流批一体的设计，通过`getBoundedness`
  来决定当前Source是流Source还是批Source，所以可以通过动态配置的方式（参考default方法）来指定一个Source既可以为流，也可以为批。
- `getProducedCatalogTables`来得到数据的schema，connector可以选择硬编码来实现固定的schema，或者实现通过用户定义的config配置来自定义schema，推荐后者。
- SeaTunnelSource是执行在driver端的类，通过该类，来获取SourceReader，SplitEnumerator等对象以及序列化器。
- 目前SeaTunnelSource支持的生产的数据类型必须是SeaTunnelRow类型。

#### SourceSplitEnumerator.java

通过该枚举器来获取数据读取的分片（SourceSplit）情况，不同的分片可能会分配给不同的SourceReader来读取数据。包含几个关键方法：

- `open`用于初始化SourceSplitEnumerator，可以在这里初始化一些连接器的资源，比如连接数据库，初始化一些状态等。
- `run`用于执行产生SourceSplit并调用`SourceSplitEnumerator.Context.assignSplit`来将分片分发给SourceReader。
- `addSplitsBack`用于处理SourceReader异常导致SourceSplit无法正常处理或者重启时，需要SourceSplitEnumerator对这些Split进行重新分发。
- `registerReader`
  处理一些在run运行了之后才注册上的SourceReader，如果这个时候还没有分发下去的SourceSplit，就可以分发给这些新的Reader（对，你大多数时候需要在SourceSplitEnumerator里面维护你的SourceSplit分发情况）
- `handleSplitRequest`
  如果有些Reader主动向SourceSplitEnumerator请求SourceSplit，那么可以通过该方法调用`SourceSplitEnumerator.Context.assignSplit`来向对应的Reader发送分片。
- `snapshotState`用于流处理定时返回需要保存的当前状态，如果有状态恢复时，会调用`SeaTunnelSource.restoreEnumerator`
  来构造SourceSplitEnumerator，将保存的状态恢复给SourceSplitEnumerator。
- `notifyCheckpointComplete`用于状态保存成功后的后续处理，可以用于将状态或者标记存入第三方存储。
- `handleSourceEvent`用于处理SourceReader的事件，可以自定义事件，比如SourceReader的状态变化等。
- `close`用于关闭SourceSplitEnumerator，释放资源。

##### SourceSplitEnumerator.Context

Context是SourceSplitEnumerator的上下文，通过该上下文来和SeaTunnel进行交互，包含几个关键方法：

- `currentParallelism`用于获取当前任务的并行度。
- `registeredReaders`用于获取当前已经注册的SourceReader列表。
- `assignSplit`用于将分片分发给SourceReader。
- `signalNoMoreSplits`用于通知某个Reader没有更多的分片了。
- `sendEventToSourceReader`用于发送事件给SourceReader。
- `getMetricsContext`用于获取当前任务的MetricsContext，用于记录一些指标。
- `getEventListener`用于获取当前任务的EventListener，用于发送事件到SeaTunnel。

#### SourceSplit.java

用于保存分片的接口，不同的分片需要定义不同的splitId，可以通过实现这个接口，保存分片需要保存的数据，比如kafka的partition和topic，hbase的columnfamily等信息，用于SourceReader来确定应该读取全部数据的哪一部分。

#### SourceReader.java

直接和数据源进行交互的接口，通过实现该接口完成从数据源读取数据的动作。

- `pollNext`便是Reader的核心，通过这个接口，实现读取数据源的数据然后返回给SeaTunnel的流程。每当准备将数据传递给SeaTunnel时，就可以调用参数中的`Collector.collect`
  方法，可以无限次的调用该方法完成数据的大量读取。但是现阶段支持的数据格式只能是`SeaTunnelRow`
  。因为我们的Source是流批一体的，所以批模式的时候Connector要自己决定什么时候结束数据读取，比如批处理一次读取100条数据，读取完成后需要在`pollNext`
  中调用`SourceReader.Context.signalNoMoreElement`
  通知SeaTunnel没有数据读取了，那么就可以利用这100条数据进行批处理。流处理没有这个要求，那么大多数流批一体的SourceReader都会出现如下代码：

```java
if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
    // signal to the source that we have reached the end of the data.
    context.signalNoMoreElement();
    break;
    }
```

代表着只有批模式的时候才会通知SeaTunnel。

- `addSplits`用于框架将SourceSplit分配给不同的SourceReader，SourceReader应该将得到的分片保存起来，然后在`pollNext`
  中读取对应的分片数据，但是可能出现Reader没有分片读取的时候（可能SourceSplit还没生成或者当前Reader确实分配不到），这个时候`pollNext`应该做出对应的处理，比如继续等待。
- `handleNoMoreSplits`触发时表示没有更多分片，需要Connector Source可选的做出相应的反馈
- `snapshotState`用于流处理定时返回需要保存的当前状态，也就是分片信息（SeaTunnel将分片信息和状态保存在一起，实现动态分配）。
- `notifyCheckpointComplete`和`notifyCheckpointAborted`和名字一样，是checkpoint不同状态下的回调。

##### SourceReader.Context

Context是SourceReader的上下文，通过该上下文来和SeaTunnel进行交互，包含几个关键方法：

- `getIndexOfSubtask`用于获取当前Reader的subTask索引。
- `getBoundedness`用于获取当前Reader的Boundedness，是流还是批。
- `signalNoMoreElement`用于通知SeaTunnel没有数据读取了。
- `sendSplitRequest`用于向SourceSplitEnumerator请求分片，用于在Reader没有分片的时候主动请求分片。
- `sendSourceEventToEnumerator`用于发送事件给SourceSplitEnumerator。
- `getMetricsContext`用于获取当前任务的MetricsContext，用于记录一些指标。
- `getEventListener`用于获取当前任务的EventListener，用于发送事件到SeaTunnel。

### Sink

#### TableSinkFactory.java

- 用于创建Sink的工厂类，通过该类来创建Sink实例，通过`createSink`方法来创建Sink实例。
- `factoryIdentifier`用于标识当前Factory的名称，也是在配置文件中配置的名称，用于区分不同的连接器。
- `optionRule` 用于定义当前连接器支持的参数，可以通过该方法来定义参数的逻辑，比如哪些参数是必须的，哪些参数是可选的，哪些参数是互斥的等等，SeaTunnel会通过OptionRule来校验用户的配置是否合法。请参考下方的Option。
- 请确保在`TableSinkFactory`添加`@AutoService(Factory.class)`注解。

#### SeaTunnelSink.java

用于定义数据写入目标端的方式，通过该接口来实现获取SinkWriter和SinkCommitter等实例。Sink端有一个重要特性就是分布式事务的处理，SeaTunnel定义了两种不同的Committer：`SinkCommitter`
用于处理针对不同的subTask进行事务的处理，每个subTask处理各自的事务，然后成功后再由`SinkAggregatedCommitter`单线程的处理所有节点的事务结果。不同的Connector
Sink可以根据组件属性进行选择，到底是只实现`SinkCommitter`或`SinkAggregatedCommitter`，还是都实现。

- `createWriter`用于创建SinkWriter实例，SinkWriter是和数据源进行交互的接口，通过该接口来将数据写入到数据源。
- `restoreWriter`用于恢复SinkWriter，用于在恢复状态时，将SinkWriter恢复到之前的状态，会在任务恢复时调用。
- `getWriteCatalogTable`用于获取Sink写入表对应的SeaTunnel CatalogTable，SeaTunnel会根据这个CatalogTable来处理指标相关的逻辑。

#### SinkWriter.java

用于直接和输出源进行交互，将SeaTunnel通过数据源取得的数据提供给Writer进行数据写入。

- `write` 负责将数据传入SinkWriter，可以选择直接写入，或者缓存到一定数据后再写入，目前数据类型只支持`SeaTunnelRow`。
- `prepareCommit` 在commit之前执行，可以在这直接写入数据，也可以实现2pc中的阶段一，然后在`SinkCommitter`或`SinkAggregatedCommitter`
  中实现阶段二。该方法返回的就是commit信息，将会提供给`SinkCommitter`和`SinkAggregatedCommitter`用于下一阶段事务处理。
- `snapshotState` 用于流处理定时返回需要保存的当前状态，如果有状态恢复时，会调用`SeaTunnelSink.restoreWriter`来构造SinkWriter，将保存的状态恢复给SinkWriter。
- `abortPrepare` 在prepareCommit失败时执行，用于回滚prepareCommit的操作。
- `close` 用于关闭SinkWriter，释放资源。

##### SinkWriter.Context

Context是SinkWriter的上下文，通过该上下文来和SeaTunnel进行交互，包含几个关键方法：

- `getIndexOfSubtask` 用于获取当前Writer的subTask索引。
- `getNumberOfParallelSubtasks` 用于获取当前任务的并行度。
- `getMetricsContext` 用于获取当前任务的MetricsContext，用于记录一些指标。
- `getEventListener` 用于获取当前任务的EventListener，用于发送事件到SeaTunnel。

#### SinkCommitter.java

用于处理`SinkWriter.prepareCommit`返回的数据信息，包含需要提交的事务信息等。和`SinkAggregatedCommitter`不同的是，`SinkCommitter`是在每个节点上执行的，我们更推荐使用`SinkAggregatedCommitter`。

- `commit` 用于提交`SinkWriter.prepareCommit`返回的事务信息，如果失败则需要实现幂等性，保存引擎重试能够正常运作。
- `abort` 用于回滚`SinkWriter.prepareCommit`的操作，如果失败则需要实现幂等性，保存引擎重试能够正常运作。

#### SinkAggregatedCommitter.java

用于处理`SinkWriter.prepareCommit`返回的数据信息，包含需要提交的事务信息等，但是会在单个节点一起处理，这样可以避免阶段二部分失败导致状态不一致的问题。

- `init` 用于初始化`SinkAggregatedCommitter`，可以在这里初始化一些连接器的资源，比如连接数据库，初始化一些状态等。
- `restoreCommit` 用于恢复`SinkAggregatedCommitter`，用于在恢复状态时，将`SinkAggregatedCommitter`恢复到之前的状态，会在任务恢复时调用，我们应该在这个方法里重新尝试提交上次未完成的事务。
- `commit` 用于提交`SinkWriter.prepareCommit`返回的事务信息，如果失败则需要实现幂等性，保存引擎重试能够正常运作。
- `combine` 用于将`SinkWriter.prepareCommit`返回的事务信息进行聚合，然后生成聚合的事务信息。
- `abort` 用于回滚`SinkWriter.prepareCommit`的操作，如果失败则需要实现幂等性，保存引擎重试能够正常运作。
- `close` 用于关闭`SinkAggregatedCommitter`，释放资源。

#### 我应该实现SinkCommitter还是SinkAggregatedCommitter？

当前版本推荐将实现SinkAggregatedCommitter作为首选，可以在Flink/Spark中提供较强的一致性保证，同时commit应该要实现幂等性，保存引擎重试能够正常运作。

### Option

当我们实现TableSourceFactory 和 TableSinkFactory时，会创建对应的Option，每一个Option对应的就是一个配置，但是不同的配置会有不同的类型，普通类型直接调用对应的方法即可创建。
但是如果我们参数类型是一个对象，我们就可以使用POJO来表示对象类型的参数，同时需要在每个参数上使用`org.apache.seatunnel.api.configuration.util.OptionMark`来表明这是一个子Option。
`OptionMark`有两个参数，`name`用于声明字段对应的参数名称，如果为空的话，我们会默认将java对应的小驼峰转换成下划线进行表达，如：`myUserPassword`->`my_user_password`。
在大多数情况下，默认为空即可。`description`用于表示当前参数的描述，这个参数是可选的，建议和文档上的保持一致。具体例子可以参考`org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkFactory`。

TableSourceFactory 和 TableSinkFactory 中的`optionRule` 返回的是参数逻辑，用于表示我们的连接器参数哪些支持，哪些参数是必须(required)的，哪些参数是可选(optional)的，哪些参数是互斥(exclusive)的，哪些参数是绑定(bundledRequired)的。
这个方法会在我们可视化创建连接器逻辑的时候用到，同时也会用于根据用户配置的参数生成完整的参数对象，然后连接器开发者就不用在Config里面一个个判断参数是否存在，直接使用即可。
可以参考现有的实现，比如`org.apache.seatunnel.connectors.seatunnel.elasticsearch.source.ElasticsearchSourceFactory`。针对很多Source都有支持配置Schema，所以采用了通用的Option，
需要Schema则可以引用`org.apache.seatunnel.api.table.catalog.CatalogTableUtil.SCHEMA`。

## 实现

现阶段所有的连接器实现及可参考的示例都在seatunnel-connectors-v2下，用户可自行查阅参考。