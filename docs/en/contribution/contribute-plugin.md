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

SeaTunnel use `plugin-mapping.properties` file to locate the name of the jar package, the file is under module `seatunnel-connectors`, the key/value rule in
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
