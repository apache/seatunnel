package io.github.interestinglab.waterdrop.pipelines

import java.io.File

import scala.collection.JavaConversions._
import com.typesafe.config.ConfigFactory
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// TODO:
// (1) pipeline 生成
// (2) pipeline 使用
// (3) pipeline viewer UI
// (4) pipeline builder UI

class Pipeline(name: String) {

  val execStartingPoint: Pipeline.StartingPoint = Pipeline.Unused // 执行起始点，执行起始点必须比子Pipeline起始点靠前
  val streamingInputList: List[BaseStreamingInput[Any]] = List()
  val staticInputList: List[BaseStaticInput] = List()
  val filterList: List[BaseFilter] = List()
  val outputList: List[BaseOutput] = List()

  // pipeline 起始点 [point1] input [point2] filter [point3] output
  val subPipelinesStartingPoint: Pipeline.StartingPoint = Pipeline.Unused // 子Pipeline起始点。
  val subPipelines: List[Pipeline] = List()

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

  def main(args: Array[String]): Unit = {

    val (rootPipeline, rootPipelineType) = pipelineBuilder(args)

    validatePipeline(rootPipeline)

    val spark = SparkSession.builder().getOrCreate()

    // TODO: 验证pipelineRunnerForStreaming直到执行ssc.start()才阻塞
    // TODO: 验证pipelineRunnerForBatch遇到action立即会阻塞
    rootPipelineType match {
      case Streaming => {

        val duration = 10
        val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(duration))
        pipelineRunnerForStreaming(rootPipeline, spark, ssc)

        ssc.start()
        ssc.awaitTermination()
      }
      case Batch => {
        pipelineRunnerForBatch(rootPipeline, spark)
      }
    }
  }

  def pipelineBuilder(args: Array[String]): (Pipeline, PipelineType) = {

    // TODO: build pipeline

    val r = """^pipeline<([0-9a-zA-Z_]+)>""".r // pipeline<pname> pattern
    val rootConfig = ConfigFactory.parseFile(new File(args(0)))
    val sparkConfig = rootConfig.getConfig("spark")

    val rootPipeline = new Pipeline("ROOT_PIPELINE")

    // 如果有input，本身就是一个pipeline
    // 如果没有input，那么它内层的pipeline必须有input
    if (rootConfig.hasPath("input")) {
      val pipeline = new Pipeline("")
    }

    for (configName <- rootConfig.root.unwrapped.keySet) {
      configName match {
        case name if name.startsWith("pipeline") => {
          val r(pipelineName) = name
          println("pipeline: " + pipelineName)
        }

        case _ => {}
      }

    }
    val pType = Streaming
    (rootPipeline, pType)
  }

  /**
   * @throws ConfigError
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

  def pipelineRunnerForStreaming(pipeline: Pipeline, spark: SparkSession, ssc: StreamingContext): Unit = {

    pipeline.execStartingPoint match {
      case PreInput => {

        processPreInputForStreaming(pipeline, spark, ssc)
      }
      case (Unused, PreFilter, PreOutput) => {
        // execution flow should not enter here.
        // **Must** throw exception --> config error, or logical error
      }
    }
  }

  def pipelineRunner(pipeline: Pipeline, spark: SparkSession, datasource: Dataset[Row]): Unit = {

    pipeline.execStartingPoint match {

      case PreFilter => {
        processPreFilter(pipeline, spark, datasource)
      }
      case PreOutput => {
        processPreOutput(pipeline, spark, datasource)
      }
      case (Unused, PreInput) => {
        // **理论上就不可能进入这个流程
        // **Must** throw Exception
      }
    }
  }

  /**
   * For static
   * Exec point: PreInput
   * */
  private def pipelineRunnerForBatch(pipeline: Pipeline, spark: SparkSession): Unit = {

    pipeline.subPipelinesStartingPoint match {
      case PreInput => {
        for (subPipeline <- pipeline.subPipelines) {
          pipelineRunnerForBatch(subPipeline, spark)
        }
      }
      case PreFilter => {
        processPreInputForStaticWithPreFilter(pipeline, spark)
      }
      case PreOutput => {
        processPreInputForStaticWithPreOutput(pipeline, spark)
      }
      case Unused => {
        processPreInputForStaticWithUnused(pipeline, spark)
      }
    }
  }

  private def processPreInputForStaticWithPreFilter(pipeline: Pipeline, spark: SparkSession): Unit = {
    // TODO: init static inputs, filter[this pipeline], subPipeline(in PreFilter)

  }

  private def processPreInputForStaticWithPreOutput(pipeline: Pipeline, spark: SparkSession): Unit = {
    // TODO: innput, filter, subPipeline(in PreOutput)

  }

  private def processPreInputForStaticWithUnused(pipeline: Pipeline, spark: SparkSession): Unit = {
    // TODO: input, filter, output
  }

  /**
   * For streaming
   * */
  private def processPreInputForStreaming(pipeline: Pipeline, spark: SparkSession, ssc: StreamingContext): Unit = {
    pipeline.subPipelinesStartingPoint match {
      case PreInput => {
        for (subPipeline <- pipeline.subPipelines) {
          pipelineRunnerForStreaming(subPipeline, spark, ssc)
        }
      }
      case PreFilter => {
        // TODO: init static inputs
        pipeline
          .streamingInputList(0)
          .start(
            spark,
            ssc,
            dataset => {

              var ds = dataset
              for (filter <- pipeline.filterList) {
                ds = filter.process(spark, ds)
              }

              for (subPipeline <- pipeline.subPipelines) {
                pipelineRunner(subPipeline, spark, ds)
              }
            }
          )
      }
      case PreOutput => {
        processPreInputForStreamingWithPreOutput(pipeline, spark, ssc)
      }
      case Unused => {
        processPreInputForStreamingWithUnused(pipeline, spark, ssc)
      }
    }
  }

  private def processPreInputForStreamingWithPreOutput(
    pipeline: Pipeline,
    spark: SparkSession,
    ssc: StreamingContext): Unit = {

    // TODO: init static inputs
    pipeline
      .streamingInputList(0)
      .start(
        spark,
        ssc,
        dataset => {

          var ds = dataset
          for (filter <- pipeline.filterList) {
            ds = filter.process(spark, ds)
          }

          for (subPipeline <- pipeline.subPipelines) {
            pipelineRunner(subPipeline, spark, ds)
          }

        }
      )
  }

  private def processPreInputForStreamingWithUnused(
    pipeline: Pipeline,
    spark: SparkSession,
    ssc: StreamingContext): Unit = {

    // TODO: init static inputs

    pipeline
      .streamingInputList(0)
      .start(
        spark,
        ssc,
        dataset => {

          var ds = dataset
          for (filter <- pipeline.filterList) {
            ds = filter.process(spark, ds)
          }

          pipeline
            .streamingInputList(0)
            .beforeOutput

          for (output <- pipeline.outputList) {
            output.process(ds)
          }

          pipeline
            .streamingInputList(0)
            .afterOutput
        }
      )
  }

  private def processPreFilter(pipeline: Pipeline, spark: SparkSession, datasource: Dataset[Row]): Unit = {
    pipeline.subPipelinesStartingPoint match {
      case PreFilter => {
        for (subPipeline <- pipeline.subPipelines) {
          pipelineRunner(subPipeline, spark, datasource)
        }
      }
      case PreOutput => {

        var dataset = datasource
        for (filter <- pipeline.filterList) {
          dataset = filter.process(spark, dataset)
        }

        for (subPipeline <- pipeline.subPipelines) {
          pipelineRunner(subPipeline, spark, dataset)
        }
      }
      case Unused => {
        // process filterList, then process outputList
        var ds = datasource
        for (filter <- pipeline.filterList) {
          ds = filter.process(spark, ds)
        }

        for (output <- pipeline.outputList) {
          output.process(ds)
        }
      }
      case _ => {
        // must throw exception
      }
    }
  }

  private def processPreOutput(pipeline: Pipeline, spark: SparkSession, datasource: Dataset[Row]): Unit = {

    pipeline.subPipelinesStartingPoint match {
      case PreOutput => {

        for (subPipeline <- pipeline.subPipelines) {
          pipelineRunner(subPipeline, spark, datasource)
        }
      }
      case Unused => {
        for (output <- pipeline.outputList) {
          output.process(datasource)
        }
      }
      case _ => {
        // must throw exception
      }
    }
  }
}
