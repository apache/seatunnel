package io.github.interestinglab.waterdrop.config

import java.util.ServiceLoader

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigRenderOptions}
import io.github.interestinglab.waterdrop.apis._
import org.antlr.v4.runtime.{ANTLRFileStream, CharStream, CommonTokenStream}
import io.github.interestinglab.waterdrop.configparser.{ConfigLexer, ConfigParser, ConfigVisitor}

import util.control.Breaks._

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
    val staticInput = this.createStaticInputs
    val streamingInputs = this.createStreamingInputs
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
          .newInstance()
          .asInstanceOf[BaseFilter]

        obj.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))

        filterList = filterList :+ obj
      })

    filterList
  }

  def createStaticInputs: List[BaseStaticInput] = {

    var inputList = List[BaseStaticInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input")

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStaticInput => {
            val input = inputObject.asInstanceOf[BaseStaticInput]
            input.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createStreamingInputs: List[BaseStreamingInput] = {

    var inputList = List[BaseStreamingInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input")

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStreamingInput => {
            val input = inputObject.asInstanceOf[BaseStreamingInput]
            input.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
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
          .newInstance()
          .asInstanceOf[BaseOutput]

        obj.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))

        outputList = outputList :+ obj
      })

    outputList
  }

  /**
   * Get full qualified class name by reflection api, ignore case.
   * */
  private def buildClassFullQualifier(name: String, classType: String): String = {

    var qualifier = name
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "input" => ConfigBuilder.InputPackage
        case "filter" => ConfigBuilder.FilterPackage
        case "output" => ConfigBuilder.OutputPackage
      }

      val services: Iterable[Plugin] =
        (ServiceLoader load classOf[BaseStaticInput]).asScala ++
          (ServiceLoader load classOf[BaseStreamingInput]).asScala ++
          (ServiceLoader load classOf[BaseFilter]).asScala ++
          (ServiceLoader load classOf[BaseOutput]).asScala

      var classFound = false
      breakable {
        for (serviceInstance <- services) {
          val clz = serviceInstance.getClass
          // get class name prefixed by package name
          val clzNameLowercase = clz.getName.toLowerCase()
          val qualifierWithPackage = packageName + "." + qualifier
          clzNameLowercase == qualifierWithPackage.toLowerCase match {
            case true => {
              qualifier = clz.getName
              classFound = true
              break
            }
            case false => // do nothing
          }
        }
      }
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
