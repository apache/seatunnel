# Purpose

This article introduces the new interface and the new code structure on account of the newly designed API for Connectors
in Apache SeaTunnel. This helps developers quickly understand API and transformation layer improvements. On the other
hand, it can guide contributors how to use the new API to develop new connectors.See
this [issue](https://github.com/apache/seatunnel/issues/1608) for details.

## Code Structure

In order to separate from the old code, we have defined new modules for execution flow. This facilitates parallel
development at the current stage, and reduces the difficulty of merging.

### engineering structure

- ../`seatunnel-connectors-v2`                                        connector-v2 code implementation
- ../`seatunnel-translation`                                          translation layer for the connector-v2
- ../`seatunnel-transform-v2`                                         transform v2 connector implementation
- ../seatunnel-e2e/`seatunnel-connector-v2-e2e`                       connector v2 e2e code
- ../seatunnel-examples/`seatunnel-engine-examples`                   seatunnel connector-v2 example use Zeta local running instance 
- ../seatunnel-examples/`seatunnel-flink-connector-v2-example`        seatunnel connector-v2 example use Flink local running instance
- ../seatunnel-examples/`seatunnel-spark-connector-v2-example`        seatunnel connector-v2 example use Spark local running instance

### Example

We have prepared three locally executable example programs in `seatunnel-examples`:
- `seatunnel-examples/seatunnel-engine-examples/src/main/java/org/apache/seatunnel/example/engine/SeaTunnelEngineLocalExample.java`, which runs on the Zeta engine
- `seatunnel-examples/seatunnel-flink-connector-v2-example/src/main/java/org/apache/seatunnel/example/flink/v2/SeaTunnelApiExample.java`, which runs on the Flink engine
- `seatunnel-examples/seatunnel-spark-connector-v2-example/src/main/java/org/apache/seatunnel/example/spark/v2/SeaTunnelApiExample.java`, which runs on the Spark engine

You can debug these examples to help you better understand the running logic of the program. The configuration files used are saved in the `resources/examples` folder.
If you want to add your own connectors, you need to follow the steps below.

To add a new connector to the example using the Zeta engine, follow these steps:
1. Add the connector dependency's `groupId`, `artifactId`, and `version` to `seatunnel-examples/seatunnel-engine-examples/pom.xml` (or to `seatunnel-examples/seatunnel-flink-connector-v2-example/pom.xml` or `seatunnel-examples/seatunnel-spark-connector-v2-example/pom.xml` if you want to run it on the Flink or Spark engine, respectively).
2. If there are dependencies in your connector with `scope` set to `test` or `provided`, add these dependencies to `seatunnel-examples/seatunnel-engine-examples/pom.xml` and change the `scope` to `compile`.
3. Add the task configuration file under `resources/examples`.
4. Configure the file path in the `SeaTunnelEngineLocalExample.java` main method.
5. Run the main method.

### Create New Seatunnel V2 Connector

1. Create a new module under the `seatunnel-connectors-v2` directory and name it connector-{ConnectorName}.
2. The `pom.xml` file can refer to the `pom.xml` file of the existing connector, and add the current sub-module to `seatunnel-connectors-v2/pom.xml`.
3. Create two packages corresponding to source and sink

    package org.apache.seatunnel.connectors.seatunnel.{ConnectorName}}.source
    package org.apache.seatunnel.connectors.seatunnel.{ConnectorName}}.sink

4. add connector info to plugin-mapping.properties file in seatunnel root path.

5. add connector dependency to seatunnel-dist/pom.xml, so the connector jar can be find in binary package.

6. There are several classes that must be implemented on the source side, namely {ConnectorName}Source, {ConnectorName}SourceFactory, {ConnectorName}SourceReader; There are several classes that must be implemented on the sink side, namely {ConnectorName}Sink, {ConnectorName}SinkFactory, {ConnectorName}SinkWriter Please refer to other connectors for details

7. {ConnectorName}SourceFactory and {ConnectorName}SinkFactory needs to be annotated with the `@AutoService (Factory.class)` annotation on the class name, and in addition to the required methods, source side an additional `creatSource` method needs to be rewritten and sink side an additional `creatSink` method needs to be rewritten

8. {ConnectorName}Source needs to override the `getProducedCatalogTables` method; {ConnectorName}Sink needs to override the `getWriteCatalogTable` method

### Startup Class

We have created three starter projects: `seatunnel-core/seatunnel-starter`, `seatunnel-core/seatunnel-flink-starter`, and `seatunnel-core/seatunnel-spark-starter`. 
Here you can find how to parse configuration files into executable Zeta/Flink/Spark processes.

### SeaTunnel API

The `seatunnel-api` module is used to store the new interfaces defined by the SeaTunnel API. By implementing these interfaces, developers can create SeaTunnel Connectors that support multiple engines.

### Translation Layer

We realize the conversion between SeaTunnel API and Engine API by adapting the interfaces of different engines, so as to
achieve the effect of translation, and let our SeaTunnel Connector support the operation of multiple different engines.
The corresponding code address, `seatunnel-translation`, this module has the corresponding translation layer
implementation. If you are interested, you can view the code and help us improve the current code.

## API introduction

The API design of the current version of SeaTunnel draws on the design concept of Flink.

### Source

#### TableSourceFactory.java

- Used to create a factory class for Source, through which Source instances are created using the `createSource` method.
- `factoryIdentifier` is used to identify the name of the current Factory, which is also configured in the configuration file to distinguish different connectors.
- `optionRule` is used to define the parameters supported by the current connector. This method can be used to define the logic of the parameters, such as which parameters are required, which are optional, which are mutually exclusive, etc. 
  SeaTunnel will use `OptionRule` to verify the validity of the user's configuration. Please refer to the `Option` below.
- Make sure to add the `@AutoService(Factory.class)` annotation to `TableSourceFactory`.

#### SeaTunnelSource.java

- The Source of SeaTunnel adopts the design of stream-batch integration, `getBoundedness` which determines whether the
  current Source is a stream Source or a batch Source, so you can specify a Source by dynamic configuration (refer to
  the default method), which can be either a stream or a batch.
- `getProducedCatalogTables` is used to get the schema of the data. The connector can choose to hard-code to implement a fixed schema or implement a custom schema through user-defined configuration. 
  The latter is recommended.
- SeaTunnelSource is a class executed on the driver side, through which objects such as SourceReader, SplitEnumerator
  and serializers are obtained.
- Currently, the data type supported by SeaTunnelSource must be SeaTunnelRow.

#### SourceSplitEnumerator.java

Use this enumerator to get the data read shard (SourceSplit) situation, different shards may be assigned to different
SourceReaders to read data. Contains several key methods:

- The `open` method is used to initialize the SourceSplitEnumerator. In this method, you can initialize resources such as database connections or states.
- `run`: Used to perform a spawn SourceSplit and call `SourceSplitEnumerator.Context.assignSplit`: to distribute the
  shards to the SourceReader.
- `addSplitsBackSourceSplitEnumerator`: is required to redistribute these Splits when SourceSplit cannot be processed
  normally or restarted due to the exception of SourceReader.
- `registerReaderProcess`: some SourceReaders that are registered after the run is run. If there is no SourceSplit
  distributed at this time, it can be distributed to these new readers (yes, you need to maintain your SourceSplit
  distribution in SourceSplitEnumerator most of the time).
- `handleSplitRequest`: If some Readers actively request SourceSplit from SourceSplitEnumerator, this method can be
  called SourceSplitEnumerator.Context.assignSplit to sends shards to the corresponding Reader.
- `snapshotState`: It is used for stream processing to periodically return the current state that needs to be saved.
  If there is a state restoration, it will be called SeaTunnelSource.restoreEnumerator to constructs a
  SourceSplitEnumerator and restore the saved state to the SourceSplitEnumerator.
- `notifyCheckpointComplete`: It is used for subsequent processing after the state is successfully saved, and can be
  used to store the state or mark in third-party storage.
- `handleSourceEvent` is used to handle events from the `SourceReader`. You can customize events, such as changes in the state of the `SourceReader`.
- `close` is used to close the `SourceSplitEnumerator` and release resources.

#### SourceSplitEnumerator.Context

The `SourceSplitEnumerator.Context` is the context for the `SourceSplitEnumerator`, which interacts with SeaTunnel. It includes several key methods:

- `currentParallelism`: Used to get the current task's parallelism.
- `registeredReaders`: Used to get the list of currently registered `SourceReader`.
- `assignSplit`: Used to assign splits to `SourceReader`.
- `signalNoMoreSplits`: Used to notify a `SourceReader` that there are no more splits.
- `sendEventToSourceReader`: Used to send events to `SourceReader`.
- `getMetricsContext`: Used to get the current task's `MetricsContext` for recording metrics.
- `getEventListener`: Used to get the current task's `EventListener` for sending events to SeaTunnel.

#### SourceSplit.java

The interface used to save shards. Different shards need to define different splitIds. You can implement this interface
to save the data that shards need to save, such as kafka's partition and topic, hbase's columnfamily and other
information, which are used by SourceReader to determine Which part of the total data should be read.

#### SourceReader.java

The interface that directly interacts with the data source, and the action of reading data from the data source is
completed by implementing this interface.

- `pollNext`: It is the core of Reader. Through this interface, the process of reading the data of the data source and
  returning it to SeaTunnel is realized. Whenever you are ready to pass data to SeaTunnel, you can call
  the `Collector.collect` method in the parameter, which can be called an infinite number of times to complete a large
  amount of data reading. But the data format supported at this stage can only be `SeaTunnelRow`. Because our Source
  is a stream-batch integration, the Connector has to decide when to end data reading in batch mode. For example, a
  batch reads 100 pieces of data at a time. After the reading is completed, it needs `pollNext` to call in
  to `SourceReader.Context.signalNoMoreElementnotify` SeaTunnel that there is no data to read . , then you can use
  these 100 pieces of data for batch processing. Stream processing does not have this requirement, so most SourceReaders
  with integrated stream batches will have the following code:

``java
if(Boundedness.BOUNDED.equals(context.getBoundedness())){
    // signal to the source that we have reached the end of the data.
    context.signalNoMoreElement();
    break;
    }
``

It means that SeaTunnel will be notified only in batch mode.

- `addSplits`:  Used by the framework to assign SourceSplit to different SourceReaders, SourceReader should save the
  obtained shards, and then pollNextread the corresponding shard data in it, but there may be times when the Reader does
  not read shards (maybe SourceSplit has not been generated or The current Reader is indeed not allocated), at this
  time, pollNextcorresponding processing should be made, such as continuing to wait.
- `handleNoMoreSplits`: When triggered, it indicates that there are no more shards, and the Connector Source is
  required to optionally make corresponding feedback
- `snapshotStateIt`: is used for stream processing to periodically return the current state that needs to be saved,
  that is, the fragmentation information (SeaTunnel saves the fragmentation information and state together to achieve
  dynamic allocation).
- `notifyCheckpointComplete`: Like `notifyCheckpointAborted` the name, it is a callback for different states of
  checkpoint.

#### SourceReader.Context

The `SourceReader.Context` is the context for the `SourceReader`, which interacts with SeaTunnel. It includes several key methods:

- `getIndexOfSubtask`: Used to get the current Reader's subTask index.
- `getBoundedness`: Used to get the current Reader's Boundedness, whether it is stream or batch.
- `signalNoMoreElement`: Used to notify SeaTunnel that there are no more elements to read.
- `sendSplitRequest`: Used to request splits from the `SourceSplitEnumerator` when the Reader has no splits.
- `sendSourceEventToEnumerator`: Used to send events to the `SourceSplitEnumerator`.
- `getMetricsContext`: Used to get the current task's `MetricsContext` for recording metrics.
- `getEventListener`: Used to get the current task's `EventListener` for sending events to SeaTunnel.

### Sink

#### TableSinkFactory.java

- Used to create a factory class for the Sink, through which Sink instances are created using the `createSink` method.
- `factoryIdentifier` is used to identify the name of the current Factory, which is also configured in the configuration file to distinguish different connectors.
- `optionRule` is used to define the parameters supported by the current connector. You can use this method to define the logic of the parameters, such as which parameters are required, which parameters are optional, which parameters are mutually exclusive, etc. SeaTunnel will use `OptionRule` to verify the validity of the user's configuration. Please refer to the Option below.
- Make sure to add the `@AutoService(Factory.class)` annotation to the `TableSinkFactory` class.

#### SeaTunnelSink.java

It is used to define the way to write data to the destination, and obtain instances such as `SinkWriter`
and `SinkCommitter` through this interface. An important feature of the sink side is the processing of distributed
transactions. SeaTunnel defines two different Committers: `SinkCommitter` used to process transactions for different
subTasks `SinkAggregatedCommitter`. Process transaction results for all nodes. Different Connector Sinks can be
selected according to component properties, whether to implement only `SinkCommitter` or `SinkAggregatedCommitter`,
or both.

- `createWriter` is used to create a `SinkWriter` instance. The `SinkWriter` is an interface that interacts with the data source, allowing data to be written to the data source through this interface.
- `restoreWriter` is used to restore the `SinkWriter` to its previous state during state recovery. This method is called when the task is restored.
- `getWriteCatalogTable` is used to get the `SeaTunnel CatalogTable` corresponding to the table written by the `Sink`. SeaTunnel will handle metrics-related logic based on this `CatalogTable`.

#### SinkWriter.java

It is used to directly interact with the output source, and provide the data obtained by SeaTunnel through the data
source to the Writer for data writing.

- `write`: Responsible for transferring data to `SinkWriter`, you can choose to write it directly, or write it after
  buffering a certain amount of data. Currently, only the data type is supported `SeaTunnelRow`.
- `prepareCommit`: Executed before commit, you can write data directly here, or you can implement phase one in 2pc,
  and then implement phase two in `SinkCommitter` or `SinkAggregatedCommitter`. What this method returns is the
  commit information, which will be provided `SinkCommitter` and `SinkAggregatedCommitter` used for the next stage
  of transaction processing.
- `snapshotState` is used to periodically return the current state to be saved during stream processing. If there is a state recovery, `SeaTunnelSink.restoreWriter` will be called to construct the `SinkWriter` and restore the saved state to the `SinkWriter`.
- `abortPrepare` is executed when `prepareCommit` fails, used to roll back the operations of `prepareCommit`.
- `close` is used to close the `SinkWriter` and release resources.

##### SinkWriter.Context

The `Context` is the context for the `SinkWriter`, which interacts with SeaTunnel. It includes several key methods:

- `getIndexOfSubtask`: Used to get the current Writer's subTask index.
- `getNumberOfParallelSubtasks`: Used to get the current task's parallelism.
- `getMetricsContext`: Used to get the current task's `MetricsContext` for recording metrics.
- `getEventListener`: Used to get the current task's `EventListener` for sending events to SeaTunnel.

#### SinkCommitter.java

Used to process the data information returned by `SinkWriter.prepareCommit`, including the transaction information that needs to be submitted. Unlike `SinkAggregatedCommitter`, `SinkCommitter` is executed on each node. We recommend using `SinkAggregatedCommitter`.

- `commit`: Used to submit the transaction information returned by `SinkWriter.prepareCommit`. If it fails, idempotency must be implemented to ensure that the engine retry can work normally.
- `abort`: Used to roll back the operations of `SinkWriter.prepareCommit`. If it fails, idempotency must be implemented to ensure that the engine retry can work normally.

#### SinkAggregatedCommitter.java

Used to process the data information returned by `SinkWriter.prepareCommit`, including the transaction information that needs to be submitted. However, it will be processed together on a single node, which can avoid the problem of inconsistency caused by the failure of the second part of the stage.

- `init`: Used to initialize the `SinkAggregatedCommitter`. You can initialize some resources for the connector here, such as connecting to a database or initializing some states.
- `restoreCommit`: Used to restore the `SinkAggregatedCommitter` to its previous state during state recovery. This method is called when the task is restored, and we should retry committing the unfinished transactions in this method.
- `commit`: Used to submit the transaction information returned by `SinkWriter.prepareCommit`. If it fails, idempotency must be implemented to ensure that the engine retry can work normally.
- `combine`: Used to aggregate the transaction information returned by `SinkWriter.prepareCommit` and then generate aggregated transaction information.
- `abort`: Used to roll back the operations of `SinkWriter.prepareCommit`. If it fails, idempotency must be implemented to ensure that the engine retry can work normally.
- `close`: Used to close the `SinkAggregatedCommitter` and release resources.

#### Implement SinkCommitter or SinkAggregatedCommitter?

In the current version, it is recommended to implement `SinkAggregatedCommitter` as the first choice, which can
provide strong consistency guarantee in Flink/Spark. At the same time, commit should be idempotent, and save engine
retry can work normally.

### Options

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

In `TableSourceFactory` and `TableSinkFactory`, the `optionRule` method returns the parameter logic, 
which defines which parameters are supported by our connector, which parameters are required, which parameters are optional, 
which parameters are mutually exclusive, and which parameters are bundled required. This method will be used when we visually create the connector logic, 
and it will also be used to generate a complete parameter object based on the user's configured parameters, so that connector developers do not need to check each parameter in the config individually and can use it directly. 
You can refer to existing implementations, such as `org.apache.seatunnel.connectors.seatunnel.elasticsearch.source.ElasticsearchSourceFactory`. For many sources that support schema configuration, a common option is used, and if a schema is needed, 
you can refer to `org.apache.seatunnel.api.table.catalog.CatalogTableUtil.SCHEMA`.

## Implement

All Connector implementations should be under the `seatunnel-connectors-v2`, and the examples that can be referred to
at this stage are under this module.