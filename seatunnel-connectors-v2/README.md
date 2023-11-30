# Purpose

This article introduces the new interface and the new code structure on account of the newly designed API for Connectors
in Apache SeaTunnel. This helps developers quickly understand API and transformation layer improvements. On the other
hand, it can guide contributors how to use the new API to develop new connectors.See
this [issue](https://github.com/apache/seatunnel/issues/1608) for details.

## **Code Structure**

In order to separate from the old code, we have defined new modules for execution flow. This facilitates parallel
development at the current stage, and reduces the difficulty of merging.

### engineering structure

- ../`seatunnel-connectors-v2`                                        connector-v2 code implementation
- ../`seatunnel-translation`                                          translation layer for the connector-v2
- ../`seatunnel-transform-v2`                                         transform v2 connector implementation
- ../seatunnel-e2e/`seatunnel-connector-v2-e2e`                       connector v2 e2e code
- ../seatunnel-examples/`seatunnel-flink-connector-v2-example`        seatunnel connector-v2 example use flink local running instance
- ../seatunnel-examples/`seatunnel-spark-connector-v2-example`        seatunnel connector-v2 example use spark local running instance

### **Example**

We have prepared two new version of the locally executable example program in `seatunnel-examples`,one
is `seatunnel-examples/seatunnel-flink-connector-v2-example/src/main/java/org/apache/seatunnel/example/flink/v2/SeaTunnelApiExample.java`
, it runs in the Flink engine. Another one
is `seatunnel-examples/seatunnel-spark-connector-v2-example/src/main/java/org/apache/seatunnel/example/spark/v2/SeaTunnelApiExample.java`
, it runs in the Spark engine. This is also the debugging method that is often used in the local development of
Connector. You can debug these examples, which will help you better understand the running logic of the program. The
configuration files used in example are saved in the "resources/examples" folder. If you want to add examples for your
own connectors, you need to follow the steps below.

1. Add the groupId, artifactId and version of the connector to be tested to
   `seatunnel-examples/seatunnel-flink-connector-v2-example/pom.xml`(or add it to
   `seatunnel-examples/seatunnel-spark-connector-v2-example/pom.xml` when you want to runs it in Spark engine) as a
   dependency.
2. Find the dependency in your connector pom file which scope is test or provided and then add them to
   seatunnel-examples/seatunnel-flink-connector-v2-example/pom.xml(or add it to
   seatunnel-examples/seatunnel-spark-connector-v2-example/pom.xml) file and modify the scope to compile.
3. Add the task configuration file under resources/examples.
4. Configure the file in the `SeaTunnelApiExample` main method.
5. Just run the main method.

### **Create new seatunnel v2 connector**

1.Create a new module under the `seatunnel-connectors-v2` directory and name it connector - {connector name}.

2.The pom file can refer to the pom file of the existing connector, and add the current sub model to the pom file of the parent model

3.Create two packages corresponding to source and sink

​    package org.apache.seatunnel.connectors.seatunnel.{connector name}}.source

​    package org.apache.seatunnel.connectors.seatunnel.{connector name}}.sink

4.add connector info to plugin-mapping.properties file in seatunnel root path.

5.add connector dependency to seatunnel-dist/pom.xml, so the connector jar can be find in binary package.

### **Startup Class**

Aside from the old startup class, we have created two new startup modules,
namely ``seatunnel-core/seatunnel-flink-starter`` and ``seatunnel-core/seatunnel-spark-starter``. You can find out how
to parse the configuration file into an executable Flink/Spark process here.

### **SeaTunnel API**

A new ``seatunnel-api`` (not ``seatunnel-apis``) module has been created to store the new interfaces defined by the
SeaTunnel API. By implementing these interfaces, developers can complete the SeaTunnel Connector that supports multiple
engines.

### **Translation Layer**

We realize the conversion between SeaTunnel API and Engine API by adapting the interfaces of different engines, so as to
achieve the effect of translation, and let our SeaTunnel Connector support the operation of multiple different engines.
The corresponding code address, ``seatunnel-translation``, this module has the corresponding translation layer
implementation. If you are interested, you can view the code and help us improve the current code.

## **API introduction**

The API design of the current version of SeaTunnel draws on the design concept of Flink.

### **Source**

#### **SeaTunnelSource.java**

- The Source of SeaTunnel adopts the design of stream-batch integration, ``getBoundedness`` which determines whether the
  current Source is a stream Source or a batch Source, so you can specify a Source by dynamic configuration (refer to
  the default method), which can be either a stream or a batch.
- ``getRowTypeInfo`` To get the schema of the data, the connector can choose to hard-code to implement a fixed schema,
  or run the user to customize the schema through config configuration. The latter is recommended.
- SeaTunnelSource is a class executed on the driver side, through which objects such as SourceReader, SplitEnumerator
  and serializers are obtained.
- Currently, the data type supported by SeaTunnelSource must be SeaTunnelRow.

#### **SourceSplitEnumerator.java**

Use this enumerator to get the data read shard (SourceSplit) situation, different shards may be assigned to different
SourceReaders to read data. Contains several key methods:

- ``run``: Used to perform a spawn SourceSplit and call ``SourceSplitEnumerator.Context.assignSplit``: to distribute the
  shards to the SourceReader.
- ``addSplitsBackSourceSplitEnumerator``: is required to redistribute these Splits when SourceSplit cannot be processed
  normally or restarted due to the exception of SourceReader.
- ``registerReaderProcess``: some SourceReaders that are registered after the run is run. If there is no SourceSplit
  distributed at this time, it can be distributed to these new readers (yes, you need to maintain your SourceSplit
  distribution in SourceSplitEnumerator most of the time).
- ``handleSplitRequest``: If some Readers actively request SourceSplit from SourceSplitEnumerator, this method can be
  called SourceSplitEnumerator.Context.assignSplit to sends shards to the corresponding Reader.
- ``snapshotState``: It is used for stream processing to periodically return the current state that needs to be saved.
  If there is a state restoration, it will be called SeaTunnelSource.restoreEnumerator to constructs a
  SourceSplitEnumerator and restore the saved state to the SourceSplitEnumerator.
- ``notifyCheckpointComplete``: It is used for subsequent processing after the state is successfully saved, and can be
  used to store the state or mark in third-party storage.

#### **SourceSplit.java**

The interface used to save shards. Different shards need to define different splitIds. You can implement this interface
to save the data that shards need to save, such as kafka's partition and topic, hbase's columnfamily and other
information, which are used by SourceReader to determine Which part of the total data should be read.

#### **SourceReader.java**

The interface that directly interacts with the data source, and the action of reading data from the data source is
completed by implementing this interface.

- ``pollNext``: It is the core of Reader. Through this interface, the process of reading the data of the data source and
  returning it to SeaTunnel is realized. Whenever you are ready to pass data to SeaTunnel, you can call
  the ``Collector.collect`` method in the parameter, which can be called an infinite number of times to complete a large
  amount of data reading. But the data format supported at this stage can only be ``SeaTunnelRow``. Because our Source
  is a stream-batch integration, the Connector has to decide when to end data reading in batch mode. For example, a
  batch reads 100 pieces of data at a time. After the reading is completed, it needs ``pollNext`` to call in
  to ``SourceReader.Context.signalNoMoreElementnotify`` SeaTunnel that there is no data to read . , then you can use
  these 100 pieces of data for batch processing. Stream processing does not have this requirement, so most SourceReaders
  with integrated stream batches will have the following code:

```java
if(Boundedness.BOUNDED.equals(context.getBoundedness())){
    // signal to the source that we have reached the end of the data.
    context.signalNoMoreElement();
    break;
    }
```

It means that SeaTunnel will be notified only in batch mode.

- ``addSplits``:  Used by the framework to assign SourceSplit to different SourceReaders, SourceReader should save the
  obtained shards, and then pollNextread the corresponding shard data in it, but there may be times when the Reader does
  not read shards (maybe SourceSplit has not been generated or The current Reader is indeed not allocated), at this
  time, pollNextcorresponding processing should be made, such as continuing to wait.
- ``handleNoMoreSplits``: When triggered, it indicates that there are no more shards, and the Connector Source is
  required to optionally make corresponding feedback
- ``snapshotStateIt``: is used for stream processing to periodically return the current state that needs to be saved,
  that is, the fragmentation information (SeaTunnel saves the fragmentation information and state together to achieve
  dynamic allocation).
- ``notifyCheckpointComplete``: Like ``notifyCheckpointAborted`` the name, it is a callback for different states of
  checkpoint.

### **Sink**

#### **SeaTunnelSink.java**

It is used to define the way to write data to the destination, and obtain instances such as ``SinkWriter``
and ``SinkCommitter`` through this interface. An important feature of the sink side is the processing of distributed
transactions. SeaTunnel defines two different Committers: ``SinkCommitter`` used to process transactions for different
subTasks ``SinkAggregatedCommitter``. Process transaction results for all nodes. Different Connector Sinks can be
selected according to component properties, whether to implement only ``SinkCommitter`` or ``SinkAggregatedCommitter``,
or both.

#### **SinkWriter.java**

It is used to directly interact with the output source, and provide the data obtained by SeaTunnel through the data
source to the Writer for data writing.

- ``write``: Responsible for transferring data to ``SinkWriter``, you can choose to write it directly, or write it after
  buffering a certain amount of data. Currently, only the data type is supported ``SeaTunnelRow``.
- ``prepareCommit``: Executed before commit, you can write data directly here, or you can implement phase one in 2pc,
  and then implement phase two in ``SinkCommitter`` or ``SinkAggregatedCommitter``. What this method returns is the
  commit information, which will be provided ``SinkCommitter`` and ``SinkAggregatedCommitter`` used for the next stage
  of transaction processing.

#### **SinkCommitter.java**

It is used to process ``SinkWriter.prepareCommit`` the returned data information, including transaction information that
needs to be submitted.

#### **SinkAggregatedCommitter.java**

It is used to process ``SinkWriter.prepareCommit`` the returned data information, including transaction information that
needs to be submitted, etc., but it will be processed together on a single node, which can avoid the problem of
inconsistency of the state caused by the failure of the second part of the stage.

- ``combine``: Used ``SinkWriter.prepareCommit`` to aggregate the returned transaction information, and then generate
  aggregated transaction information.

#### **Implement SinkCommitter or SinkAggregatedCommitter?**

In the current version, it is recommended to implement ``SinkAggregatedCommitter`` as the first choice, which can
provide strong consistency guarantee in Flink/Spark. At the same time, commit should be idempotent, and save engine
retry can work normally.

### TableSourceFactory and TableSinkFactory

In order to automatically create the Source Connector and Sink Connector and Transform Connector, we need the connector to return the parameters needed to create them and the verification rules for each parameter. For Source Connector and Sink Connector, we define `TableSourceFactory` and `TableSinkFactory`
supported by the current connector and the required parameters. We define TableSourceFactory and TableSinkFactory,
It is recommended to put it in the same directory as the implementation class of SeaTunnelSource or SeaTunnelSink for easy searching.

- `factoryIdentifier` is used to indicate the name of the current Factory. This value should be the same as the 
    value returned by `getPluginName`, so that if Factory is used to create Source/Sink in the future,
    A seamless switch can be achieved.
- `createSink` and `createSource` are the methods for creating Source and Sink respectively, 
    and do not need to be implemented at present.
- `optionRule` returns the parameter logic, which is used to indicate which parameters of our connector are supported,
    which parameters are required, which parameters are optional, and which parameters are exclusive, which parameters are bundledRequired.
    This method will be used when we visually create the connector logic, and it will also be used to generate a complete parameter 
    object according to the parameters configured by the user, and then the connector developer does not need to judge whether the parameters
    exist one by one in the Config, and use it directly That's it.
    You can refer to existing implementations, such as `org.apache.seatunnel.connectors.seatunnel.elasticsearch.source.ElasticsearchSourceFactory`.
    There is support for configuring Schema for many Sources, so a common Option is used.
    If you need a schema, you can refer to `org.apache.seatunnel.api.table.catalog.CatalogTableUtil.SCHEMA`.

Don't forget to add `@AutoService(Factory.class)` to the class. This Factory is the parent class of TableSourceFactory and TableSinkFactory.

### **Options**

When we implement TableSourceFactory and TableSinkFactory, the corresponding Option will be created.
Each Option corresponds to a configuration, but different configurations will have different types. 
Common types can be created by directly calling the corresponding method.
But if our parameter type is an object, we can use POJO to represent parameters of object type,
and need to use `org.apache.seatunnel.api.configuration.util.OptionMark` on each parameter to indicate that this is A child Option.
`OptionMark` has two parameters, `name` is used to declare the parameter name corresponding to the field.
If it is empty, we will convert the small camel case corresponding to java to underscore by default, such as: `myUserPassword`  -> `my_user_password` .
In most cases, the default is empty. `description` is used to indicate the description of the current parameter.
This parameter is optional. It is recommended to be consistent with the documentation. For specific examples,
please refer to `org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkFactory`.

## **Result**

All Connector implementations should be under the ``seatunnel-connectors-v2``, and the examples that can be referred to
at this stage are under this module.


