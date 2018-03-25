package io.github.interestinglab.waterdrop.config

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigRenderOptions}
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseInput, BaseOutput}
import org.antlr.v4.runtime.{ANTLRFileStream, CharStream, CommonTokenStream}
import io.github.interestinglab.waterdrop.configparser.{ConfigLexer, ConfigParser, ConfigVisitor}

class ConfigBuilder(configFile: String) {

  val config = load()

  def load(): Config = {

    // val configFile = System.getProperty("config.path", "")
    if (configFile == "") {
      throw new ConfigRuntimeException("Please specify config file")
    }

    println("[INFO] Loading config file: " + configFile)

    // CharStreams is for Antlr4.7
    // val charStream: CharStream = CharStreams.fromFileName(configFile)
    val charStream: CharStream = new ANTLRFileStream(configFile)
    val lexer: ConfigLexer = new ConfigLexer(charStream)
    val tokens: CommonTokenStream = new CommonTokenStream(lexer)
    val parser: ConfigParser = new ConfigParser(tokens)

    val configContext: ConfigParser.ConfigContext = parser.config
    val visitor: ConfigVisitor[Config] = new ConfigVisitorImpl

    val parsedConfig = visitor.visit(configContext)

    val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
    System.out.println("[INFO] Parsed Config: \n" + parsedConfig.root().render(options))

    parsedConfig
  }

  /**
   * check if config is valid.
   * */
  def checkConfig: Unit = {
    val sparkConfig = this.getSparkConfigs
    val inputs = this.createInputs
    val outputs = this.createOutputs
    val filters = this.createFilters
  }

  def getSparkConfigs: Config = {
    config.getConfig("spark")
  }

  def createFilters: List[BaseFilter] = {

    var filterList = List[BaseFilter]()
    config
      .getConfigList("filter")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "filter")

        val obj = Class
          .forName(className)
          .getConstructor(classOf[Config])
          .newInstance(plugin.getConfig(ConfigBuilder.PluginParamsKey))
          .asInstanceOf[BaseFilter]

        filterList = filterList :+ obj
      })

    filterList
  }

  def createInputs: List[BaseInput] = {

    var inputList = List[BaseInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input")

        val obj = Class
          .forName(className)
          .getConstructor(classOf[Config])
          .newInstance(plugin.getConfig(ConfigBuilder.PluginParamsKey))
          .asInstanceOf[BaseInput]

        inputList = inputList :+ obj
      })

    inputList
  }

  def createOutputs: List[BaseOutput] = {

    var outputList = List[BaseOutput]()
    config
      .getConfigList("output")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output")

        val obj = Class
          .forName(className)
          .getConstructor(classOf[Config])
          .newInstance(plugin.getConfig(ConfigBuilder.PluginParamsKey))
          .asInstanceOf[BaseOutput]

        outputList = outputList :+ obj
      })

    outputList
  }

  private def buildClassFullQualifier(name: String, classType: String): String = {
    var qualifier = name;
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "input" => ConfigBuilder.InputPackage
        case "filter" => ConfigBuilder.FilterPackage
        case "output" => ConfigBuilder.OutputPackage
      }

      qualifier = packageName + "." + qualifier.capitalize
    }

    qualifier
  }
}

object ConfigBuilder {

  val PackagePrefix = "io.github.interestinglab.waterdrop"
  val FilterPackage = PackagePrefix + ".filter"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"

  val PluginNameKey = "name"
  val PluginParamsKey = "entries"
}
