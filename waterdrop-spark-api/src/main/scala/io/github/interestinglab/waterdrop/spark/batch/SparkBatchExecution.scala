package io.github.interestinglab.waterdrop.spark.batch

import java.util.{List => JList}

import com.typesafe.config.waterdrop.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.{CheckResult, ConfigRuntimeException}
import io.github.interestinglab.waterdrop.env.Execution
import io.github.interestinglab.waterdrop.spark.{BaseSparkSink, BaseSparkSource, BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class SparkBatchExecution(environment: SparkEnvironment)
  extends Execution[SparkBatchSource, SparkBatchTransform, SparkBatchSink] {

  private var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}

  override def start(sources: JList[SparkBatchSource],
                     transforms: JList[SparkBatchTransform],
                     sinks: JList[SparkBatchSink]): Unit = {

    sources.foreach(s => {
      SparkBatchExecution.registerInputTempView(s.asInstanceOf[BaseSparkSource[Dataset[Row]]], environment)
    })
    if (!sources.isEmpty) {
      var ds = sources.get(0).getData(environment)
      for (tf <- transforms) {

        if (ds.take(1).length > 0) {
          ds = SparkBatchExecution.transformProcess(environment, tf, ds)
          SparkBatchExecution.registerTransformTempView(tf, ds)
        }
      }

      // if (ds.take(1).length > 0) {
      sinks.foreach(sink => {
        SparkBatchExecution.sinkProcess(environment, sink, ds)
      })
      //      }
    }
  }

}


object SparkBatchExecution {

  private[waterdrop] val SOURCE_TABLE_NAME = "source_table_name"
  private[waterdrop] val RESULT_TABLE_NAME = "result_table_name"

  private[waterdrop] def registerTempView(tableName: String, ds: Dataset[Row]): Unit = {
    ds.createOrReplaceTempView(tableName)
  }

  private[waterdrop] def registerInputTempView(source: BaseSparkSource[Dataset[Row]], environment: SparkEnvironment): Unit = {
    val conf = source.getConfig
    conf.hasPath(SparkBatchExecution.RESULT_TABLE_NAME) match {
      case true => {
        val tableName = conf.getString(SparkBatchExecution.RESULT_TABLE_NAME)
        registerTempView(tableName, source.getData(environment))
      }
      case false => {
        throw new ConfigRuntimeException(
          "Plugin[" + source.name + "] must be registered as dataset/table, please set \"result_table_name\" config")

      }
    }
  }

  private[waterdrop] def transformProcess(environment: SparkEnvironment, transform: BaseSparkTransform, ds: Dataset[Row]): Dataset[Row] = {
    val config = transform.getConfig()
    val fromDs = config.hasPath(SparkBatchExecution.SOURCE_TABLE_NAME) match {
      case true => {
        val sourceTableName = config.getString(SparkBatchExecution.SOURCE_TABLE_NAME)
        environment.getSparkSession.read.table(sourceTableName)
      }
      case false => ds
    }

    transform.process(fromDs, environment)
  }

  private[waterdrop] def registerTransformTempView(plugin: BaseSparkTransform, ds: Dataset[Row]): Unit = {
    val config = plugin.getConfig()
    if (config.hasPath(SparkBatchExecution.RESULT_TABLE_NAME)) {
      val tableName = config.getString(SparkBatchExecution.RESULT_TABLE_NAME)
      registerTempView(tableName, ds)
    }
  }

  private[waterdrop] def sinkProcess(environment: SparkEnvironment, sink: BaseSparkSink[_], ds: Dataset[Row]): Unit = {
    val config = sink.getConfig()
    val fromDs = config.hasPath(SparkBatchExecution.SOURCE_TABLE_NAME) match {
      case true => {
        val sourceTableName = config.getString(SparkBatchExecution.SOURCE_TABLE_NAME)
        environment.getSparkSession.read.table(sourceTableName)
      }
      case false => ds
    }

    sink.output(fromDs, environment)
  }
}
