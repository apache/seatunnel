package org.interestinglab.waterdrop.config

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.antlr.v4.runtime.{CharStream, CharStreams, CommonTokenStream}
import org.interestinglab.waterdrop.configparser.{ConfigLexer, ConfigParser, ConfigVisitor}
import org.interestinglab.waterdrop.filter.BaseFilter
import org.interestinglab.waterdrop.input.BaseInput
import org.interestinglab.waterdrop.output.BaseOutput

class ConfigBuilder {

  val PLUGIN_NAME_KEY = "name"
  val PLUGIN_PARAMS_KEY = "entries"

  val config = load();

  def load(): Config = {

    val configFile = System.getProperty("config.file", "");
    if (configFile == "") {
      // TODO: 不写return,该怎么写？
      // TODO: 如何抛出异常
    }

    val charStream: CharStream = CharStreams.fromFileName(configFile)
    val lexer: ConfigLexer = new ConfigLexer(charStream)
    val tokens: CommonTokenStream = new CommonTokenStream(lexer)
    val parser: ConfigParser = new ConfigParser(tokens)

    val configContext: ConfigParser.ConfigContext = parser.config
    val visitor: ConfigVisitor[Config] = new ConfigVisitorImpl

    val parsedConfig = visitor.visit(configContext)

    val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
    System.out.println("Parsed Config: \n" + parsedConfig.root().render(options))

    parsedConfig
  }

  def createFilters(): List[BaseFilter] = {

    var filterList = List[BaseFilter]()
    config
      .getConfigList("filter")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(PLUGIN_NAME_KEY), "filter")

        val obj = Class
          .forName(className)
          .getConstructor(classOf[Config])
          .newInstance(plugin.getConfig(PLUGIN_PARAMS_KEY))
          .asInstanceOf[BaseFilter]

        filterList = filterList :+ obj
      })

    filterList
  }

  def createInputs(): List[BaseInput] = {

    var inputList = List[BaseInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(PLUGIN_NAME_KEY), "input")

        val obj = Class
          .forName(className)
          .getConstructor(classOf[Config])
          .newInstance(plugin.getConfig(PLUGIN_PARAMS_KEY))
          .asInstanceOf[BaseInput]

        inputList = inputList :+ obj
      })

    inputList
  }

  def createOutputs(): List[BaseOutput] = {

    var outputList = List[BaseOutput]()
    config
      .getConfigList("output")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(PLUGIN_NAME_KEY), "output")

        val obj = Class
          .forName(className)
          .getConstructor(classOf[Config])
          .newInstance(plugin.getConfig(PLUGIN_PARAMS_KEY))
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

  val PackagePrefix = "org.interestinglab.waterdrop"
  val FilterPackage = PackagePrefix + ".filter"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"
}
