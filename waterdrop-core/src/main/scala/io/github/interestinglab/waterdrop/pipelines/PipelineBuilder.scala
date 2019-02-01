package io.github.interestinglab.waterdrop.pipelines

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config.{ConfigBuilder, ConfigRuntimeException}
import io.github.interestinglab.waterdrop.pipelines.Pipeline._

import scala.util.control.Breaks.{break, breakable}

object PipelineBuilder {

  val PackagePrefix = "io.github.interestinglab.waterdrop"
  val FilterPackage = PackagePrefix + ".filter"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"

  val PluginNameKey = "name"
  val PluginParamsKey = "entries"

  def recursiveBuilder(config: Config, pname: String): (Pipeline, PipelineType, StartingPoint) = {

    val pipeline = new Pipeline(pname)

    if (config.hasPath("input")) {
      pipeline.streamingInputList = createStreamingInputs(config.getConfig("input"))
      pipeline.staticInputList = createStaticInputs(config.getConfig("input"))
    }

    if (pipeline.streamingInputList.nonEmpty || pipeline.staticInputList.nonEmpty) {
      pipeline.execStartingPoint = PreInput
    }

    if (config.hasPath("filter")) {
      pipeline.filterList = createFilters(config.getConfig("filter"))
    }

    if (pipeline.execStartingPoint != PreInput && pipeline.filterList.nonEmpty) {
      pipeline.execStartingPoint = PreFilter
    }

    if (config.hasPath("output")) {
      pipeline.outputList = createOutputs(config.getConfig("output"))
    }

    if (pipeline.execStartingPoint != PreInput
      && pipeline.execStartingPoint != PreFilter
      && pipeline.outputList.nonEmpty) {
      pipeline.execStartingPoint = PreOutput
    }

    // subPipelineStartingPoint alignment order: PreInput --> PreFilter --> PreOutput
    var subPipelineStartingPoint: StartingPoint = Unused
    var subPipelineType: PipelineType = Unknown
    val r = """^pipeline<([0-9a-zA-Z_]+)>""".r // pipeline<pname> pattern
    for (configName <- config.root.unwrapped.keySet) {
      configName match {
        case name if name.startsWith("pipeline") => {

          val r(pipelineName) = name
          println("pipeline: " + pipelineName)

          val (subPipeline, pType, subSP) = recursiveBuilder(config.getConfig(name), pipelineName)

          pipeline.subPipelines = pipeline.subPipelines :+ subPipeline

          subPipelineStartingPoint = mergeStartingPoint(subPipelineStartingPoint, subSP)

          subPipelineType = mergePipelineType(subPipelineType, pType)
        }
        case _ => {}
      }
    }

    val pType = (pipeline.streamingInputList.nonEmpty, pipeline.staticInputList.nonEmpty) match {
      case (true, _) => Streaming
      case (false, true) => Batch
      case _ => subPipelineType
    }

    (pipeline, pType, pipeline.execStartingPoint)
  }

  private def mergePipelineType(t1: PipelineType, t2: PipelineType): PipelineType = {
    (t1, t2) match {
      case (Streaming, _) => Streaming
      case (_, Streaming) => Streaming
      case (Batch, _) => Batch
      case (_, Batch) => Batch
      case _ => Unknown
    }
  }

  private def mergeStartingPoint(s1: StartingPoint, s2: StartingPoint): StartingPoint = {
    (s1, s2) match {
      case (PreInput, _) => PreInput
      case (_, PreInput) => PreInput
      case (PreFilter, _) => PreFilter
      case (_, PreFilter) => PreFilter
      case (PreOutput, _) => PreOutput
      case (_, PreOutput) => PreOutput
      case _ => Unused
    }
  }

  private def createStreamingInputs(config: Config): List[BaseStreamingInput[Any]] = {
    var inputList = List[BaseStreamingInput[Any]]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input")

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStreamingInput[Any] => {
            val input = inputObject.asInstanceOf[BaseStreamingInput[Any]]
            input.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  private def createStaticInputs(config: Config): List[BaseStaticInput] = {
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

  private def createFilters(config: Config): List[BaseFilter] = {
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

  private def createOutputs(config: Config): List[BaseOutput] = {
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
   * @throws ConfigRuntimeException
   * */
  def validatePipeline(pipeline: Pipeline): Unit = {
    // TODO: validate multiple level pipeline
    //      (1) 内层的startingpoint必须后置于外层的
    //      (2) 每个pipeline必须有datasource 血统。
    //      (3) 对于一个pipeline，如果他有input，那么它的subpipeline不能再有input。
    //            对于一个pipeline，如果他有output，那么它不能再有subpipeline。
    //            对于一个pipeline，如果他有filter，那么它的subpipeline可以再有filter。
    //      (4) 所有pipeline datasource要么都是streaming的，要么都不是。
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
          (ServiceLoader load classOf[BaseStreamingInput[Any]]).asScala ++
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
