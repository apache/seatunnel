# FAQs

## Why should I install a computing engine like Spark or Flink?

SeaTunnel now uses computing engines such as Spark and Flink to complete resource scheduling and node communication, so we can focus on the ease of use of data synchronization and the development of high-performance components. But this is only temporary.

## I have a question, and I cannot solve it by myself

I have encountered a problem when using SeaTunnel and I cannot solve it by myself. What should I do? First, search in [Issue List](https://github.com/apache/seatunnel/issues) or [Mailing List](https://lists.apache.org/list.html?dev@seatunnel.apache.org) to see if someone has already asked the same question and got an answer. If you cannot find an answer to your question, you can contact community members for help in [These Ways](https://github.com/apache/seatunnel#contact-us).

## How do I declare a variable?

Do you want to know how to declare a variable in SeaTunnel's configuration, and then dynamically replace the value of the variable at runtime?

Since `v1.2.4`, SeaTunnel supports variable substitution in the configuration. This feature is often used for timing or non-timing offline processing to replace variables such as time and date. The usage is as follows:

Configure the variable name in the configuration. Here is an example of sql transform (actually, anywhere in the configuration file the value in `'key = value'` can use the variable substitution):

```
...
transform {
  sql {
    query = "select * from user_view where city ='"${city}"' and dt = '"${date}"'"
  }
}
...
```

Taking Spark Local mode as an example, the startup command is as follows:

```bash
./bin/start-seatunnel-spark.sh \
-c ./config/your_app.conf \
-e client \
-m local[2] \
-i city=shanghai \
-i date=20190319
```

You can use the parameter `-i` or `--variable` followed by `key=value` to specify the value of the variable, where the key needs to be same as the variable name in the configuration.

## How do I write a configuration item in multi-line text in the configuration file?

When a configured text is very long and you want to wrap it, you can use three double quotes to indicate its start and end:

```
var = """
 whatever you want
"""
```

## How do I implement variable substitution for multi-line text?

It is a little troublesome to do variable substitution in multi-line text, because the variable cannot be included in three double quotation marks:

```
var = """
your string 1
"""${you_var}""" your string 2"""
```

Refer to: [lightbend/config#456](https://github.com/lightbend/config/issues/456).

## Is SeaTunnel supported in Azkaban, Oozie, DolphinScheduler?

Of course! See the screenshot below:

![workflow.png](../images/workflow.png)

![azkaban.png](../images/azkaban.png)

## Does SeaTunnel have a case for configuring multiple sources, such as configuring elasticsearch and hdfs in source at the same time?

```
env {
	...
}

source {
  hdfs { ... }	
  elasticsearch { ... }
  jdbc {...}
}

transform {
    ...
}

sink {
	elasticsearch { ... }
}
```

## Are there any HBase plugins?

There is a HBase input plugin. You can download it from here: https://github.com/garyelephant/waterdrop-input-hbase .

## How can I use SeaTunnel to write data to Hive?

```
env {
  spark.sql.catalogImplementation = "hive"
  spark.hadoop.hive.exec.dynamic.partition = "true"
  spark.hadoop.hive.exec.dynamic.partition.mode = "nonstrict"
}

source {
  sql = "insert into ..."
}

sink {
    // The data has been written to hive through the sql source. This is just a placeholder, it does not actually work.
    stdout {
        limit = 1
    }
}
```

In addition, SeaTunnel has implemented a `Hive` output plugin after version `1.5.7` in `1.x` branch; in `2.x` branch. The Hive plugin for the Spark engine has been supported from version `2.0.5`: https://github.com/apache/seatunnel/issues/910.

## How does SeaTunnel write multiple instances of ClickHouse to achieve load balancing?

1. Write distributed tables directly (not recommended)

2. Add a proxy or domain name (DNS) in front of multiple instances of ClickHouse:

   ```
   {
       output {
           clickhouse {
               host = "ck-proxy.xx.xx:8123"
               # Local table
               table = "table_name"
           }
       }
   }
   ```
3. Configure multiple instances in the configuration:

   ```
   {
       output {
           clickhouse {
               host = "ck1:8123,ck2:8123,ck3:8123"
               # Local table
               table = "table_name"
           }
       }
   }
   ```
4. Use cluster mode:

   ```
   {
       output {
           clickhouse {
               # Configure only one host
               host = "ck1:8123"
               cluster = "clickhouse_cluster_name"
               # Local table
               table = "table_name"
           }
       }
   }
   ```

## How can I solve OOM when SeaTunnel consumes Kafka?

In most cases, OOM is caused by not having a rate limit for consumption. The solution is as follows:

For the current limit of Spark consumption of Kafka:

1. Suppose the number of partitions of Kafka `Topic 1` you consume with KafkaStream = N.

2. Assuming that the production speed of the message producer (Producer) of `Topic 1` is K messages/second, the speed of write messages to the partition must be uniform.

3. Suppose that, after testing, it is found that the processing capacity of Spark Executor per core per second is M.

The following conclusions can be drawn:

1. If you want to make Spark's consumption of `Topic 1` keep up with its production speed, then you need `spark.executor.cores` * `spark.executor.instances` >= K / M

2. When a data delay occurs, if you want the consumption speed not to be too fast, resulting in spark executor OOM, then you need to configure `spark.streaming.kafka.maxRatePerPartition` <= (`spark.executor.cores` * `spark.executor.instances`) * M / N

3. In general, both M and N are determined, and the conclusion can be drawn from 2: The size of `spark.streaming.kafka.maxRatePerPartition` is positively correlated with the size of `spark.executor.cores` * `spark.executor.instances`, and it can be increased while increasing the resource `maxRatePerPartition` to speed up consumption.

![Kafka](../images/kafka.png)

## How can I solve the Error `Exception in thread "main" java.lang.NoSuchFieldError: INSTANCE`?

The reason is that the version of httpclient.jar that comes with the CDH version of Spark is lower, and The httpclient version that ClickHouse JDBC is based on is 4.5.2, and the package versions conflict. The solution is to replace the jar package that comes with CDH with the httpclient-4.5.2 version.

## The default JDK of my Spark cluster is JDK7. After I install JDK8, how can I specify that SeaTunnel starts with JDK8?

In SeaTunnel's config file, specify the following configuration:

```shell
spark {
 ...
 spark.executorEnv.JAVA_HOME="/your/java_8_home/directory"
 spark.yarn.appMasterEnv.JAVA_HOME="/your/java_8_home/directory"
 ...
}
```

## What should I do if OOM always appears when running SeaTunnel in Spark local[*] mode?

If you run in local mode, you need to modify the `start-seatunnel.sh` startup script. After `spark-submit`, add a parameter `--driver-memory 4g` . Under normal circumstances, local mode is not used in the production environment. Therefore, this parameter generally does not need to be set during On YARN. See: [Application Properties](https://spark.apache.org/docs/latest/configuration.html#application-properties) for details.

## Where can I place self-written plugins or third-party jdbc.jars to be loaded by SeaTunnel?

Place the Jar package under the specified structure of the plugins directory:

```bash
cd SeaTunnel
mkdir -p plugins/my_plugins/lib
cp third-part.jar plugins/my_plugins/lib
```

`my_plugins` can be any string.

## How do I configure logging-related parameters in SeaTunnel-V1(Spark)?

There are three ways to configure logging-related parameters (such as Log Level):

- [Not recommended] Change the default `$SPARK_HOME/conf/log4j.properties`.
  - This will affect all programs submitted via `$SPARK_HOME/bin/spark-submit`.
- [Not recommended] Modify logging related parameters directly in the Spark code of SeaTunnel.
  - This is equivalent to hardcoding, and each change needs to be recompiled.
- [Recommended] Use the following methods to change the logging configuration in the SeaTunnel configuration file (The change only takes effect if SeaTunnel >= 1.5.5 ):

  ```
  env {
      spark.driver.extraJavaOptions = "-Dlog4j.configuration=file:<file path>/log4j.properties"
      spark.executor.extraJavaOptions = "-Dlog4j.configuration=file:<file path>/log4j.properties"
  }
  source {
    ...
  }
  transform {
   ...
  }
  sink {
    ...
  }
  ```

The contents of the log4j configuration file for reference are as follows:

```
$ cat log4j.properties
log4j.rootLogger=ERROR, console

# set the log level for these components
log4j.logger.org=ERROR
log4j.logger.org.apache.spark=ERROR
log4j.logger.org.spark-project=ERROR
log4j.logger.org.apache.hadoop=ERROR
log4j.logger.io.netty=ERROR
log4j.logger.org.apache.zookeeper=ERROR

# add a ConsoleAppender to the logger stdout to write to the console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
# use a simple message format
log4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
```

## How do I configure logging related parameters in SeaTunnel-V2(Spark, Flink)?

Currently, they cannot be set directly. you need to modify the SeaTunnel startup script. The relevant parameters are specified in the task submission command. For specific parameters, please refer to the official documents:

- Spark official documentation: http://spark.apache.org/docs/latest/configuration.html#configuring-logging
- Flink official documentation: https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/logging.html

Reference:

https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console

http://spark.apache.org/docs/latest/configuration.html#configuring-logging

https://medium.com/@iacomini.riccardo/spark-logging-configuration-in-yarn-faf5ba5fdb01

## How do I configure logging related parameters of SeaTunnel-E2E Test?

The log4j configuration file of `seatunnel-e2e` existed in `seatunnel-e2e/seatunnel-e2e-common/src/test/resources/log4j2.properties`. You can modify logging related parameters directly in the configuration file.

For example, if you want to output more detailed logs of E2E Test, just downgrade `rootLogger.level` in the configuration file.

## Error when writing to ClickHouse: ClassCastException

In SeaTunnel, the data type will not be actively converted. After the Input reads the data, the corresponding
Schema. When writing ClickHouse, the field type needs to be strictly matched, and the mismatch needs to be resolved.

Data conversion can be achieved through the following two plugins:

1. Filter Convert plugin
2. Filter Sql plugin

Detailed data type conversion reference: [ClickHouse Data Type Check List](https://interestinglab.github.io/seatunnel-docs/#/en/configuration/output-plugins/Clickhouse?id=clickhouse-data-type-check-list)

Refer to issue:[#488](https://github.com/apache/seatunnel/issues/488) [#382](https://github.com/apache/seatunnel/issues/382).

## How does SeaTunnel access kerberos-authenticated HDFS, YARN, Hive and other resources?

Please refer to: [#590](https://github.com/apache/seatunnel/issues/590).

## How do I troubleshoot NoClassDefFoundError, ClassNotFoundException and other issues?

There is a high probability that there are multiple different versions of the corresponding Jar package class loaded in the Java classpath, because of the conflict of the load order, not because the Jar is really missing. Modify this SeaTunnel startup command, adding the following parameters to the spark-submit submission section, and debug in detail through the output log.

```
spark-submit --verbose
    ...
   --conf 'spark.driver.extraJavaOptions=-verbose:class'
   --conf 'spark.executor.extraJavaOptions=-verbose:class'
    ...
```

## I want to learn the source code of SeaTunnel. Where should I start?

SeaTunnel has a completely abstract and structured code implementation, and many people have chosen SeaTunnel As a way to learn Spark. You can learn the source code from the main program entry: SeaTunnel.java

## When SeaTunnel developers develop their own plugins, do they need to understand the SeaTunnel code? Should these plugins be integrated into the SeaTunnel project?

The plugin developed by the developer has nothing to do with the SeaTunnel project and does not need to include your plugin code.

The plugin can be completely independent from SeaTunnel project, so you can write it using Java, Scala, Maven, sbt, Gradle, or whatever you want. This is also the way we recommend developers to develop plugins.

## When I import a project, the compiler has the exception "class not found `org.apache.seatunnel.shade.com.typesafe.config.Config`"

Run `mvn install` first. In the `seatunnel-config/seatunnel-config-base` subproject, the package `com.typesafe.config` has been relocated to `org.apache.seatunnel.shade.com.typesafe.config` and installed to the maven local repository in the subproject `seatunnel-config/seatunnel-config-shade`.
