package io.github.interestinglab.waterdrop.config

import java.io.File

import com.typesafe.config.waterdrop.{Config, ConfigFactory, ConfigRenderOptions, ConfigResolveOptions}
import io.github.interestinglab.waterdrop.apis.{BaseSink, BaseSource, BaseTransform}
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.env.{Execution, RuntimeEnv}
import io.github.interestinglab.waterdrop.flink.stream.{FlinkStreamEnvironment, FlinkStreamExecution}
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

  def createStaticInputs: List[BaseSource[_, _]] = {
    var inputList = List[BaseSource[_, _]]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className =
          buildClassFullQualifier(
            plugin.getString(ConfigBuilder.PluginNameKey),
            "source"
          )

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseSource[_, _]]

        obj.setConfig(plugin)

        inputList = inputList :+ obj
      })

    inputList

  }

  def createFilters: List[BaseTransform[_, _, _]] = {

    var filterList = List[BaseTransform[_, _, _]]()
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
          .asInstanceOf[BaseTransform[_, _, _]]

        obj.setConfig(plugin)

        filterList = filterList :+ obj
      })

    filterList
  }

  def createRuntimeEnv(engine: String): RuntimeEnv = {

    val env = engine match {
      case "spark" => new SparkEnvironment()
      case "flink" => new FlinkStreamEnvironment()
    }
    env.setConfig(config)
    env
  }

  /**
    * check if config is valid.
    **/
  def checkConfig: Unit = {
    this.getSparkConfigs
    this.createStaticInputs
    this.createOutputs
    this.createFilters
  }


  def createExecution: (RuntimeEnv, Execution[BaseSource[_, _], BaseTransform[_, _, _], BaseSink[_, _, _]]) = {
    val env = engine match {
      case "spark" => new SparkEnvironment()
      case "flink" => new FlinkStreamEnvironment()
    }
    env.setConfig(config)

    engine match {
      case "flink" => (env, new FlinkStreamExecution(env.asInstanceOf[FlinkStreamEnvironment]).asInstanceOf[Execution[BaseSource[_, _],
        BaseTransform[_, _, _],
        BaseSink[_, _, _]]])
      case "spark" => (env, new SparkBatchExecution(env.asInstanceOf[SparkEnvironment]).asInstanceOf[Execution[BaseSource[_, _],
        BaseTransform[_, _, _],
        BaseSink[_, _, _]]])
    }
  }

  def createOutputs: List[BaseSink[_, _, _]] = {

    var outputList = List[BaseSink[_, _, _]]()
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
          .asInstanceOf[BaseSink[_, _, _]]

        obj.setConfig(plugin)

        outputList = outputList :+ obj
      })

    outputList
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
