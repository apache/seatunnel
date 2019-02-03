package io.github.interestinglab.waterdrop.pipelines

import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.pipelines.Pipeline.{PreFilter, PreInput, PreOutput, Unused}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

object PipelineRunner {

  /**
   * The order of call prepare of plugins is undefined, don't rely on this !
   * */
  def preparePipelineRecursively(spark: SparkSession, pipeline: Pipeline): Unit = {
    val plugins = pipeline.streamingInputList ::: pipeline.staticInputList ::: pipeline.filterList ::: pipeline.outputList ::: Nil
    for (plugin <- plugins) {
      plugin.prepare(spark)
    }

    for (subPipe <- pipeline.subPipelines) {
      preparePipelineRecursively(spark, subPipe)
    }
  }

  def pipelineRunnerForStreaming(pipeline: Pipeline, spark: SparkSession, ssc: StreamingContext): Unit = {

    pipeline.execStartingPoint match {
      case PreInput => {

        processPreInputForStreaming(pipeline, spark, ssc)
      }
      case Unused | PreFilter | PreOutput => {
        // execution flow should not enter here.
        // **Must** throw exception --> config error, or logical error
      }
    }
  }

  private def pipelineRunner(pipeline: Pipeline, spark: SparkSession, datasource: Dataset[Row]): Unit = {

    pipeline.execStartingPoint match {

      case PreFilter => {
        processPreFilter(pipeline, spark, datasource)
      }
      case PreOutput => {
        processPreOutput(pipeline, spark, datasource)
      }
      case Unused | PreInput => {
        // **理论上就不可能进入这个流程
        // **Must** throw Exception
      }
    }
  }

  /**
   * For static
   * Exec point: PreInput
   * */
  def pipelineRunnerForBatch(pipeline: Pipeline, spark: SparkSession): Unit = {

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
