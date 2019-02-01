package io.github.interestinglab.waterdrop.pipelines

import java.io.File

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException
import org.apache.spark.sql.{Dataset, Row, SparkSession}
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

    val (rootPipeline, rootPipelineType, _) = pipelineRecursiveBuilder(rootConfig, "ROOT_PIPELINE")

    validatePipeline(rootPipeline)

    val spark = SparkSession.builder().getOrCreate()

    preparePipelineRecursively(rootPipeline)

    // TODO: 验证pipelineRunnerForStreaming直到执行ssc.start()才阻塞
    // TODO: 验证pipelineRunnerForBatch遇到action立即会阻塞
    rootPipelineType match {

      // TODO: empty dataset
      // TODO: multiple action need cache() ???

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

  private def pipelineRecursiveBuilder(config: Config, pname: String): (Pipeline, PipelineType, StartingPoint) = {

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

          val (subPipeline, pType, subSP) = pipelineRecursiveBuilder(config.getConfig(name), pipelineName)

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

  private def preparePipelineRecursively(pipeline: Pipeline): Unit = {
    // TODO:
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

    initStaticInputs(spark, pipeline.staticInputList)

    if (pipeline.staticInputList.nonEmpty) {
      var ds = pipeline.staticInputList.head.getDataset(spark)

      for (f <- pipeline.filterList) {
        if (ds.take(1).length > 0) {
          ds = f.process(spark, ds)
        }
      }

      for (subPipeline <- pipeline.subPipelines) {
        pipelineRunner(subPipeline, spark, ds)
      }

    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }

  }

  private def processPreInputForStaticWithPreOutput(pipeline: Pipeline, spark: SparkSession): Unit = {

    initStaticInputs(spark, pipeline.staticInputList)

    if (pipeline.staticInputList.nonEmpty) {
      var ds = pipeline.staticInputList.head.getDataset(spark)

      for (f <- pipeline.filterList) {
        if (ds.take(1).length > 0) {
          ds = f.process(spark, ds)
        }
      }

      for (subPipeline <- pipeline.subPipelines) {
        pipelineRunner(subPipeline, spark, ds)
      }

    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }

  }

  private def processPreInputForStaticWithUnused(pipeline: Pipeline, spark: SparkSession): Unit = {

    initStaticInputs(spark, pipeline.staticInputList)

    if (pipeline.staticInputList.nonEmpty) {
      var ds = pipeline.staticInputList.head.getDataset(spark)

      for (f <- pipeline.filterList) {
        if (ds.take(1).length > 0) {
          ds = f.process(spark, ds)
        }
      }
      pipeline.outputList.foreach(p => {
        p.process(ds)
      })

    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }

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

        initStaticInputs(spark, pipeline.staticInputList)

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

    initStaticInputs(spark, pipeline.staticInputList)

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

    initStaticInputs(spark, pipeline.staticInputList)

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

  private def initStaticInputs(sparkSession: SparkSession, staticInputs: List[BaseStaticInput]): Unit = {

    // let static input register as table for later use if needed
    var datasetMap = Map[String, Dataset[Row]]()
    for (input <- staticInputs) {

      val ds = input.getDataset(sparkSession)

      val config = input.getConfig()
      config.hasPath("table_name") match {
        case true => {
          val tableName = config.getString("table_name")

          datasetMap.contains(tableName) match {
            case true =>
              throw new ConfigRuntimeException(
                "Detected duplicated Dataset["
                  + tableName + "], it seems that you configured table_name = \"" + tableName + "\" in multiple static inputs")
            case _ => datasetMap += (tableName -> ds)
          }

          ds.createOrReplaceTempView(tableName)
        }
        case false => {
          throw new ConfigRuntimeException(
            "Plugin[" + input.name + "] must be registered as dataset/table, please set \"table_name\" config")
        }
      }
    }
  }
}
