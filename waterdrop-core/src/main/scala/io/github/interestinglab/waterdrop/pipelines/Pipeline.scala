package io.github.interestinglab.waterdrop.pipelines

import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}

class Pipeline(name: String) {

  def getName: String = name

  // Pipeline execution starting point, execution starting point **must be** prior to subpipeline subPipelinesStartingPoint
  var execStartingPoint: Pipeline.StartingPoint = Pipeline.Unused
  var streamingInputList: List[BaseStreamingInput[Any]] = List()
  var staticInputList: List[BaseStaticInput] = List()
  var filterList: List[BaseFilter] = List()
  var outputList: List[BaseOutput] = List()

  // pipeline execution starting point [PreInput] input [PreFilter] filter [PreOutput] output
  var subPipelinesStartingPoint: Pipeline.StartingPoint = Pipeline.Unused
  var subPipelines: List[Pipeline] = List()

  override def toString: String = {
    ("[Pipeline: %s] execStartingPoint: %s, subPipelinesStartingPoint: %s, streamingInputList.size: %d, " +
      "staticInputList.size: %d, filterList.size: %d, outputList.size: %d, subPipelines.size: %d")
      .format(
        name,
        execStartingPoint.getClass.getSimpleName,
        subPipelinesStartingPoint.getClass.getSimpleName,
        streamingInputList.size,
        staticInputList.size,
        filterList.size,
        outputList.size,
        subPipelines.size
      )
  }
}

object Pipeline {

  sealed trait StartingPoint {
    def order: Int
  }
  case object PreInput extends StartingPoint {
    override def order: Int = 1
  }
  case object PreFilter extends StartingPoint {
    override def order: Int = 2
  }
  case object PreOutput extends StartingPoint {
    override def order: Int = 3
  }
  case object Unused extends StartingPoint {
    override def order: Int = {
      val ord = 4
      ord
    }
  }

  sealed trait PipelineType
  case object Streaming extends PipelineType
  case object Batch extends PipelineType
  case object Unknown extends PipelineType
}
