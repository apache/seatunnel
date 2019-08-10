package io.github.interestinglab.waterdrop.spark.config

import java.io.File

import com.typesafe.config.{
  Config,
  ConfigFactory,
  ConfigRenderOptions,
  ConfigResolveOptions
}
import io.github.interestinglab.waterdrop.apis.{
  BaseSink,
  BaseSource,
  BaseTransform
}
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls

class ConfigBuilder(configFile: String) {

  val config = load()

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

  /**
    * check if config is valid.
    * */
  def checkConfig: Unit = {
    val sparkConfig = this.getSparkConfigs
    val staticInput = this.createStaticInputs
    val outputs = this.createOutputs
    val filters = this.createFilters
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

  private def getInputType(name: String, engine: String): String = {
    name match {
      case _ if name.toLowerCase.endsWith("stream") => {
        engine match {
          case "batch"               => "sparkstreaming"
          case "structuredstreaming" => "structuredstreaming"
        }
      }
      case _ => "batch"
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
        case "source"    => ConfigBuilder.SourcePackage
        case "transform" => ConfigBuilder.TransformPackage
        case "sink"      => ConfigBuilder.SinkPackage
      }
      return packageName + "." + qualifier
    }

    qualifier
  }
}

object ConfigBuilder {

  val PackagePrefix = "io.github.interestinglab.waterdrop.spark.batch"
  val SourcePackage = PackagePrefix + ".source"
  val TransformPackage = PackagePrefix + ".transform"
  val SinkPackage = PackagePrefix + ".sink"

  val PluginNameKey = "plugin_name"

}
