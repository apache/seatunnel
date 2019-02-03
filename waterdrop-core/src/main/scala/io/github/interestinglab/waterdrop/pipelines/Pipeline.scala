package io.github.interestinglab.waterdrop.pipelines

import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}

// TODO:
// [done] (1) pipeline 生成
// [done] (2) pipeline 使用
// (3) pipeline viewer UI (+ Spark Tab)
// (4) pipeline builder UI
// (5) 数据仓库刷新分区的需求。
// (6) 考虑 end-to-end exactly once 实现方案。

class Pipeline(name: String) {

  def getName: String = name

  var execStartingPoint: Pipeline.StartingPoint = Pipeline.Unused // 执行起始点，执行起始点必须比子Pipeline起始点靠前
  var streamingInputList: List[BaseStreamingInput[Any]] = List()
  var staticInputList: List[BaseStaticInput] = List()
  var filterList: List[BaseFilter] = List()
  var outputList: List[BaseOutput] = List()

  // pipeline 起始点 [point1] input [point2] filter [point3] output
  var subPipelinesStartingPoint: Pipeline.StartingPoint = Pipeline.Unused // 子Pipeline起始点。
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

  sealed trait PipelineType {}
  case object Streaming extends PipelineType
  case object Batch extends PipelineType
  case object Unknown extends PipelineType
}
