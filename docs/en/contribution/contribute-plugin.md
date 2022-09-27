# Contribute Spark Plugins

There are two parent modules for Spark plugins:

1. [seatunnel-connectors-spark](https://github.com/apache/incubator-seatunnel/tree/dev/seatunnel-connectors/seatunnel-connectors-spark)
2. [seatunnel-transforms-spark](https://github.com/apache/incubator-seatunnel/tree/dev/seatunnel-transforms/seatunnel-transforms-spark)

Once you want to contribute a new plugin, you need to:

## Create plugin module
Create your plugin module under the corresponding parent plugin module.
For example, if you want to add a new Spark connector plugin, you need to create a new module under the `seatunnel-connectors-spark` module.

```xml

<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-connectors-spark</artifactId>
        <version>${revision}</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>seatunnel-connector-spark-hello</artifactId>

    <dependencies>
        <dependency>
            <groupId>org.apache.seatunnel</groupId>
            <artifactId>seatunnel-api-spark</artifactId>
            <version>${project.version}</version>
        </dependency>
    </dependencies>
</project>
```
## Add plugin implementation

You need to implement the `Connector` service provider interface. e.g. `BaseSource`/`BaseSink`.

Conveniently, there are some abstract class can help you easy to create your plugin. If you want to create a source connector,
you can implement with `SparkBatchSource`/`SparkStreamingSource`. If you want to create a sink connector, you can implement with `SparkBatchSink`/`SparkStreamingSink`.

The methods defined in `SparkBatchSource` are some lifecycle methods. will be executed by SeaTunnel engine.
The execution order of the lifecycle methods is: `checkConfig` -> `prepare` -> `getData` -> `close`.

```java
import java.util.Date;

@AutoService(BaseSparkSource.class)
public class Hello extends SparkBatchSource {
    @Override
    public Dataset<Row> getData(SparkEnvironment env) {
        // do your logic here to generate data
        Dataset<Row> dataset = null;
        return dataset;
    }

    @Override
    public CheckResult checkConfig() {
        return super.checkConfig();
    }

    @Override
    public void prepare(SparkEnvironment env) {
        super.prepare(env);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public String getPluginName() {
        return "hello";
    }
}
```

- The `getPluginName` method is used to identify the plugin name.
- The `@AutoService` is used to generate the `META-INF/services/org.apache.seatunnel.BaseSparkSource` file
  automatically.

Since this process cannot work on scala, if you use slala to implement your plugin, you need to add a service provider
to the `META-INF/services` file. The file name should be `org.apache.seatunnel.spark.BaseSparkSource`
or `org.apache.seatunnel.spark.BaseSparkSink`, dependents on the plugin type. The content of the file should be the
fully qualified class name of your implementation.

## Add plugin to the distribution

You need to add your plugin to the `seatunnel-connectors-spark-dist` module, then the plugin will in distribution.

```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-connector-spark-hello</artifactId>
    <version>${project.version}</version>
</dependency>
```

After you using `mvn package` to make a distribution, you can find the plugin in your ${distribution}/connectors/spark.

## Add information to plugin-mapping.properties file

SeaTunnel use `plugin-mapping.properties` file to locate the name of the jar package, the file is under the parent module `incubator-seatunnel`, the key/value rule in
properties is : `engineName.pluginType.pluginName=artifactId`. eg: `spark.source.hello=seatunnel-connector-spark-hello`.
So that SeaTunnel can find plugin jar according to user's config file.

# Contribute Flink Plugins

The steps to contribute a Flink plugin is similar to the steps to contribute a Spark plugin. Different from Spark, you
need to add your plugin in Flink plugin modules.

# Add e2e tests for your plugin

Once you add a new plugin, it is recommended to add e2e tests for it. We have a `seatunnel-e2e` module to help you to do
this.

For example, if you want to add an e2e test for your flink connector, you can create a new test in `seatunnel-flink-e2e`
module. And extend the FlinkContainer class in the test.

```java
public class HellpSourceIT extends FlinkContainer {

    @Test
    public void testHellpSource() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/hello/hellosource.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // do some other assertion here
    }
}

```
Once your class implements the `FlinkContainer` interface, it will auto create a Flink cluster in Docker, and you just need to
execute the `executeSeaTunnelFlinkJob` method with your SeaTunnel configuration file, it will submit the SeaTunnel job.

In most times, you need to start a third-part datasource in your test, for example, if you add a clickhouse connectors, you may need to 
start a Clickhouse database in your test. You can use `GenericContainer` to start a container.

It should be noted that your e2e test class should be named ending with `IT`. By default, we will not execute the test if the class name ending with `IT`.
You can add `-DskipIT=false` to execute the e2e test, it will rely on a Docker environment.

# Connector-v2 Contribution Guide

## Purpose

The following describes the new interface and the new code structure on account of the newly designed API for Connectors in Apache SeaTunnel. This helps developers with quick overview regarding API, translation layer improvement, and development of new Connector.

## Code Structure

In order to separate from the old code, we have defined new modules for execution flow. This facilitates parallel development at the current stage, and reduces the difficulty of merging. All the relevant code at this stage is kept on the ``api-draft`` branch.

### Example

We have prepared a new version of the locally executable example program in ``seatunnel-examples``, which can be directly called using ``seatunnel-flink-connector-v2-example`` or ``seatunnel-spark-connector-v2-example`` in ``SeaTunnelApiExample``. This is also the debugging method that is often used in the local development of Connector. The corresponding configuration files are saved in the same module ``resources/examples`` folder as before.


### Startup Class

Aside from the old startup class, we have created two new startup class projects, namely ``seatunnel-core/seatunnel-flink-starter`` and ``seatunnel-core/seatunnel-spark-starter``. You can find out how to parse the configuration file into an executable Flink/Spark process here.

### SeaTunnel API

A new ``seatunnel-api`` (not ``seatunnel-apis``) module has been created to store the new interfaces defined by the SeaTunnel API. By implementing these interfaces, developers can complete the SeaTunnel Connector that supports multiple engines.

### Translation Layer

We realize the conversion between SeaTunnel API and Engine API by adapting the interfaces of different engines, so as to achieve the effect of translation, and let our SeaTunnel Connector support the operation of multiple different engines. The corresponding code address, ``seatunnel-translation``, this module has the corresponding translation layer implementation. If you are interested, you can view the code and help us improve the current code.

## API introduction

The API design of the current version of SeaTunnel draws on the design concept of Flink.

### Source

#### SeaTunnelSource.java

- The Source of SeaTunnel adopts the design of stream-batch integration, ``getBoundedness`` which determines whether the current Source is a stream Source or a batch Source, so you can specify a Source by dynamic configuration (refer to the default method), which can be either a stream or a batch.
- ``getRowTypeInfo`` To get the schema of the data, the connector can choose to hard-code to implement a fixed schema, or run the user to customize the schema through config configuration. The latter is recommended.
- SeaTunnelSource is a class executed on the driver side, through which objects such as SourceReader, SplitEnumerator and serializers are obtained.
- Currently, the data type supported by SeaTunnelSource must be SeaTunnelRow.

#### SourceSplitEnumerator.java

Use this enumerator to get the data read shard (SourceSplit) situation, different shards may be assigned to different SourceReaders to read data. Contains several key methods:

- ``run``: Used to perform a spawn SourceSplit and call ``SourceSplitEnumerator.Context.assignSplit``: to distribute the shards to the SourceReader.
- ``addSplitsBackSourceSplitEnumerator``: is required to redistribute these Splits when SourceSplit cannot be processed normally or restarted due to the exception of SourceReader.
- ``registerReaderProcess``: some SourceReaders that are registered after the run is run. If there is no SourceSplit distributed at this time, it can be distributed to these new readers (yes, you need to maintain your SourceSplit distribution in SourceSplitEnumerator most of the time).
- ``handleSplitRequest``: If some Readers actively request SourceSplit from SourceSplitEnumerator, this method can be called SourceSplitEnumerator.Context.assignSplit to sends shards to the corresponding Reader.
- ``snapshotState``: It is used for stream processing to periodically return the current state that needs to be saved. If there is a state restoration, it will be called SeaTunnelSource.restoreEnumerator to constructs a SourceSplitEnumerator and restore the saved state to the SourceSplitEnumerator.
- ``notifyCheckpointComplete``: It is used for subsequent processing after the state is successfully saved, and can be used to store the state or mark in third-party storage.

#### SourceSplit.java

The interface used to save shards. Different shards need to define different splitIds. You can implement this interface to save the data that shards need to save, such as kafka's partition and topic, hbase's columnfamily and other information, which are used by SourceReader to determine Which part of the total data should be read.

#### SourceReader.java

The interface that directly interacts with the data source, and the action of reading data from the data source is completed by implementing this interface.

- ``pollNext``: It is the core of Reader. Through this interface, the process of reading the data of the data source and returning it to SeaTunnel is realized. Whenever you are ready to pass data to SeaTunnel, you can call the ``Collector.collect`` method in the parameter, which can be called an infinite number of times to complete a large amount of data reading. But the data format supported at this stage can only be ``SeaTunnelRow``. Because our Source is a stream-batch integration, the Connector has to decide when to end data reading in batch mode. For example, a batch reads 100 pieces of data at a time. After the reading is completed, it needs ``pollNext`` to call in to ``SourceReader.Context.signalNoMoreElementnotify`` SeaTunnel that there is no data to read . , then you can use these 100 pieces of data for batch processing. Stream processing does not have this requirement, so most SourceReaders with integrated stream batches will have the following code:

      if ( Boundedness . BOUNDED . equals ( context . getBoundedness ())) 
      {
      // signal to the source that we have reached the end of the data. context . signalNoMoreElement ();
      break ;
      }

It means that SeaTunnel will be notified only in batch mode.

- ``addSplits``:  Used by the framework to assign SourceSplit to different SourceReaders, SourceReader should save the obtained shards, and then pollNextread the corresponding shard data in it, but there may be times when the Reader does not read shards (maybe SourceSplit has not been generated or The current Reader is indeed not allocated), at this time, pollNextcorresponding processing should be made, such as continuing to wait.
- ``handleNoMoreSplits``: When triggered, it indicates that there are no more shards, and the Connector Source is required to optionally make corresponding feedback
- ``snapshotStateIt``: is used for stream processing to periodically return the current state that needs to be saved, that is, the fragmentation information (SeaTunnel saves the fragmentation information and state together to achieve dynamic allocation).
- ``notifyCheckpointComplete``: Like ``notifyCheckpointAborted`` the name, it is a callback for different states of checkpoint.

### Sink

#### SeaTunnelSink.java

It is used to define the way to write data to the destination, and obtain instances such as ``SinkWriter`` and ``SinkCommitter`` through this interface. An important feature of the sink side is the processing of distributed transactions. SeaTunnel defines two different Committers: ``SinkCommitter`` used to process transactions for different subTasks ``SinkAggregatedCommitter``. Process transaction results for all nodes. Different Connector Sinks can be selected according to component properties, whether to implement only ``SinkCommitter`` or ``SinkAggregatedCommitter``, or both.

#### SinkWriter.java

It is used to directly interact with the output source, and provide the data obtained by SeaTunnel through the data source to the Writer for data writing.

- ``write``: Responsible for transferring data to ``SinkWriter``, you can choose to write it directly, or write it after buffering a certain amount of data. Currently, only the data type is supported ``SeaTunnelRow``.
- ``prepareCommit``: Executed before commit, you can write data directly here, or you can implement phase one in 2pc, and then implement phase two in ``SinkCommitter`` or ``SinkAggregatedCommitter``. What this method returns is the commit information, which will be provided ``SinkCommitter`` and ``SinkAggregatedCommitter`` used for the next stage of transaction processing.

#### SinkCommitter.java

It is used to process ``SinkWriter.prepareCommit`` the returned data information, including transaction information that needs to be submitted.

#### SinkAggregatedCommitter.java

It is used to process ``SinkWriter.prepareCommit`` the returned data information, including transaction information that needs to be submitted, etc., but it will be processed together on a single node, which can avoid the problem of inconsistency of the state caused by the failure of the second part of the stage.

- ``combine``: Used ``SinkWriter.prepareCommit`` to aggregate the returned transaction information, and then generate aggregated transaction information.

#### Implement SinkCommitter or SinkAggregatedCommitter?

In the current version, it is recommended to implement ``SinkAggregatedCommitter`` as the first choice, which can provide strong consistency guarantee in Flink/Spark. At the same time, commit should be idempotent, and save engine retry can work normally.

## Result

All Connector implementations should be under the ``seatunnel-connectors/seatunnel-connectors-spark/flink/flink-sql``module, and the examples that can be referred to at this stage are under this module.

