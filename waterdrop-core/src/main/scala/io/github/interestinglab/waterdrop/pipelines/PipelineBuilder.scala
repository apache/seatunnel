package io.github.interestinglab.waterdrop.pipelines

import scala.collection.JavaConversions._

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.pipelines.Pipeline._

object PipelineBuilder {

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
    // TODO:
    List()
  }

  private def createStaticInputs(config: Config): List[BaseStaticInput] = {
    // TODO:
    List()
  }

  private def createFilters(config: Config): List[BaseFilter] = {
    // TODO:
    List()
  }

  private def createOutputs(config: Config): List[BaseOutput] = {
    // TODO:
    List()
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
}
