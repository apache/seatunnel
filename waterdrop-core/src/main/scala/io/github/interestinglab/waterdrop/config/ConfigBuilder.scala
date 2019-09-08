package io.github.interestinglab.waterdrop.config

import java.io.File

import com.typesafe.config.waterdrop.{Config, ConfigFactory, ConfigRenderOptions, ConfigResolveOptions}
import io.github.interestinglab.waterdrop.apis.{BaseSink, BaseSource, BaseTransform}
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.env.{Execution, RuntimeEnv}
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamExecution
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchExecution

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls


class ConfigBuilder(configFile: String, engine: String) {


  def this(configFile: String) {
    this(configFile, "")
  }

  val config = load()
  val configPackage = new ConfigPackage(engine)

  def load(): Config = {

    // val configFile = System.getProperty("config.path", "")
    if (configFile == "") {
      throw new ConfigRuntimeException("Please specify config file")
    }

    println("[INFO] Loading config file: " + configFile)

    // variables substitution / variables resolution order:
    // onfig file --> syste environment --> java properties
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


  def getSparkConfigs: Config = {
    config.getConfig("spark")
  }

  def createSources: (List[BaseSource], Boolean) = {
    var sourceList = List[BaseSource]()

    val sourceConfigList = config.getConfigList("input")

    val isStreaming = sourceConfigList.get(0).getString(ConfigBuilder.PluginNameKey).endsWith("Streaming")

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
      .getConfigList("filter")
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
      .getConfigList("output")
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
    this.getSparkConfigs
    this.createSources
    this.createSinks
    this.createTransforms
  }

  def createExecution: (RuntimeEnv, Execution[BaseSource, BaseTransform, BaseSink]) = {
    val env = engine match {
      case "spark" => new SparkEnvironment()
      case "flink" => new FlinkEnvironment()
    }
    env.setConfig(config)

    engine match {
      case "flink" => (env, new FlinkStreamExecution(env.asInstanceOf[FlinkEnvironment]).asInstanceOf[Execution[BaseSource,
        BaseTransform,
        BaseSink]])
      case "spark" => (env, new SparkBatchExecution(env.asInstanceOf[SparkEnvironment]).asInstanceOf[Execution[BaseSource,
        BaseTransform,
        BaseSink]])
    }
  }

  /**
    * Get full qualified class name by reflection api, ignore case.
    * */
  private def buildClassFullQualifier(name: String,
                                      classType: String): String = {
    buildClassFullQualifier(name, classType, "")
  }

  private def buildClassFullQualifier(name: String,
                                      classType: String,
                                      engine: String): String = {

    val qualifier = name
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "source"    => configPackage.sourcePackage
        case "transform" => configPackage.transformPackage
        case "sink"      => configPackage.sinkPackage
      }
      packageName + "." + qualifier
    } else {
      qualifier
    }
  }
}

object ConfigBuilder {

  val PluginNameKey = "plugin_name"

}
