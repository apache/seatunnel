package io.github.interestinglab.waterdrop.pipelines

import java.io.File

import com.typesafe.config.ConfigFactory
import io.github.interestinglab.waterdrop.pipelines.Pipeline.{Batch, Streaming}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PipelineMain {
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
