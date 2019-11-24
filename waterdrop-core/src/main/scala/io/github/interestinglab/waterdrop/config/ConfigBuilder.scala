package io.github.interestinglab.waterdrop.config

import java.io.File

import com.typesafe.config.waterdrop.{Config, ConfigFactory, ConfigRenderOptions, ConfigResolveOptions}
import io.github.interestinglab.waterdrop.apis.{BaseSink, BaseSource, BaseTransform}
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.env.{Execution, RuntimeEnv}
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchExecution
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamExecution
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchExecution
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingExecution

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls


class ConfigBuilder(configFile: String, engine: String) {


  def this(configFile: String) {
    this(configFile, "")
  }

  val config = load()
  val configPackage = new ConfigPackage(engine)

  def load(): Config = {

    if (configFile == "") {
      throw new ConfigRuntimeException("Please specify config file")
    }

    println("[INFO] Loading config file: " + configFile)

    // variables substitution / variables resolution order:
    // config file --> system environment --> java properties
    val config = ConfigFactory
      .parseFile(new File(configFile))
      .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
      .resolveWith(
        ConfigFactory.systemProperties,
        ConfigResolveOptions.defaults.setAllowUnresolved(true)
      )

    val options: ConfigRenderOptions =
      ConfigRenderOptions.concise.setFormatted(true)
    println("[INFO] parsed config file: " + config.root().render(options))

    config
  }


  def getEnvConfigs: Config = {
    config.getConfig("env")
  }

  def createSources: (List[BaseSource], Boolean) = {
    var sourceList = List[BaseSource]()

    val sourceConfigList = config.getConfigList("source")

    val isStreaming = sourceConfigList.get(0).getString(ConfigBuilder.PluginNameKey).toLowerCase.endsWith("stream")

    sourceConfigList
      .foreach(plugin => {
        val className =
          buildClassFullQualifier(
            plugin.getString(ConfigBuilder.PluginNameKey),
            "source"
          )

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseSource]

        obj.setConfig(plugin)

        sourceList = sourceList :+ obj
      })

    (sourceList, isStreaming)

  }

  def createTransforms: List[BaseTransform] = {

    var filterList = List[BaseTransform]()
    config
      .getConfigList("transform")
      .foreach(plugin => {
        val className =
          buildClassFullQualifier(
            plugin.getString(ConfigBuilder.PluginNameKey),
            "transform"
          )

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseTransform]

        obj.setConfig(plugin)

        filterList = filterList :+ obj
      })

    filterList
  }

  def createSinks: List[BaseSink] = {

    var outputList = List[BaseSink]()
    config
      .getConfigList("sink")
      .foreach(plugin => {

        val className =
          buildClassFullQualifier(
            plugin.getString(ConfigBuilder.PluginNameKey),
            "sink"
          )

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseSink]

        obj.setConfig(plugin)

        outputList = outputList :+ obj
      })

    outputList
  }

  /**
    * check if config is valid.
    **/
  def checkConfig: Unit = {
    this.getEnvConfigs
    this.createSources
    this.createSinks
    this.createTransforms
  }

  def createExecution(isStreaming: Boolean): (RuntimeEnv, Execution[BaseSource, BaseTransform, BaseSink]) = {
    val env = engine match {
      case "spark" => new SparkEnvironment()
      case "flink" => new FlinkEnvironment()
    }

    env.prepare(isStreaming)
    env.setConfig(config.getConfig("env"))

    engine match {
      case "flink" => {
        if (isStreaming) {
          (env, new FlinkStreamExecution(env.asInstanceOf[FlinkEnvironment]).asInstanceOf[Execution[BaseSource,
            BaseTransform,
            BaseSink]])
        } else {
          (env, new FlinkBatchExecution(env.asInstanceOf[FlinkEnvironment]).asInstanceOf[Execution[BaseSource,
            BaseTransform,
            BaseSink]])
        }
      }
      case "spark" => {
        if (isStreaming) {
          (env, new SparkStreamingExecution(env.asInstanceOf[SparkEnvironment]).asInstanceOf[Execution[BaseSource,
            BaseTransform,
            BaseSink]])
        } else {
          (env, new SparkBatchExecution(env.asInstanceOf[SparkEnvironment]).asInstanceOf[Execution[BaseSource,
            BaseTransform,
            BaseSink]])
        }
      }
    }
  }

  /**
    * Get full qualified class name by reflection api, ignore case.
    **/

  private def buildClassFullQualifier(name: String, classType: String): String = {

    val qualifier = name
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "source" => configPackage.sourcePackage
        case "transform" => configPackage.transformPackage
        case "sink" => configPackage.sinkPackage
      }
      val firstUppercase = qualifier.substring(0,1).toUpperCase + qualifier.substring(1)
      packageName + "." + firstUppercase
    } else {
      qualifier
    }
  }
}

object ConfigBuilder {

  val PluginNameKey = "plugin_name"

}
