package io.github.interestinglab.waterdrop.pipelines

import java.io.File

import com.typesafe.config.ConfigFactory
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

// TODO:
// [done] (1) pipeline 生成
// [done] (2) pipeline 使用
// (3) pipeline viewer UI (+ Spark Tab)
// (4) pipeline builder UI

class Pipeline(name: String) {

  var execStartingPoint: Pipeline.StartingPoint = Pipeline.Unused // 执行起始点，执行起始点必须比子Pipeline起始点靠前
  var streamingInputList: List[BaseStreamingInput[Any]] = List()
  var staticInputList: List[BaseStaticInput] = List()
  var filterList: List[BaseFilter] = List()
  var outputList: List[BaseOutput] = List()

  // pipeline 起始点 [point1] input [point2] filter [point3] output
  var subPipelinesStartingPoint: Pipeline.StartingPoint = Pipeline.Unused // 子Pipeline起始点。
  var subPipelines: List[Pipeline] = List()

  // TODO: 数据仓库刷新分区的需求。
  // TODO: 考虑 end-to-end exactly once 实现方案。
}

object Pipeline {

  sealed trait StartingPoint {}
  case object PreInput extends StartingPoint
  case object PreFilter extends StartingPoint
  case object PreOutput extends StartingPoint
  case object Unused extends StartingPoint

  sealed trait PipelineType {}
  case object Streaming extends PipelineType
  case object Batch extends PipelineType
  case object Unknown extends PipelineType

  def main(args: Array[String]): Unit = {

    val rootConfig = ConfigFactory.parseFile(new File(args(0)))
    val sparkConfig = rootConfig.getConfig("spark")

    val (rootPipeline, rootPipelineType, _) = PipelineBuilder.recursiveBuilder(rootConfig, "ROOT_PIPELINE")

    PipelineBuilder.validatePipeline(rootPipeline)

    val spark = SparkSession.builder().getOrCreate()

    PipelineRunner.preparePipelineRecursively(spark, rootPipeline)

    // TODO: 验证pipelineRunnerForStreaming直到执行ssc.start()才阻塞
    // TODO: 验证pipelineRunnerForBatch遇到action立即会阻塞
    rootPipelineType match {

      // TODO: empty dataset
      // TODO: multiple action need cache() ???

      case Streaming => {

        val duration = 10
        val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(duration))
        PipelineRunner.pipelineRunnerForStreaming(rootPipeline, spark, ssc)

        ssc.start()
        ssc.awaitTermination()
      }
      case Batch => {
        PipelineRunner.pipelineRunnerForBatch(rootPipeline, spark)
      }
    }
  }

}
