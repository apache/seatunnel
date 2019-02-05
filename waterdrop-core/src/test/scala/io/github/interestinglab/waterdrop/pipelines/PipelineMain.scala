package io.github.interestinglab.waterdrop.pipelines

import java.io.File

import com.typesafe.config.ConfigFactory
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.pipelines.Pipeline.{Batch, Streaming, Unknown}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PipelineMain {
  def main(args: Array[String]): Unit = {

    val rootConfig = ConfigFactory.parseFile(new File(args(0)))
    val sparkConfig = rootConfig.getConfig("spark")

    val (rootPipeline, rootPipelineType, _) = PipelineBuilder.recursiveBuilder(rootConfig, "ROOT_PIPELINE")

    rootPipelineType match {
      case Unknown => {
        throw new ConfigRuntimeException("Cannot not detect pipeline type, please check your config")
      }
      case _ => {}
    }

    println("rootPipeline: " + rootPipeline)

    println("rootPipelineType: " + rootPipelineType)

    PipelineBuilder.validatePipeline(rootPipeline)

    PipelineBuilder.checkConfigRecursively(rootPipeline)

    val spark = SparkSession.builder().getOrCreate()

    PipelineRunner.preparePipelineRecursively(spark, rootPipeline)

    // pipelineRunnerForStreaming直到执行ssc.start()才阻塞
    // pipelineRunnerForBatch遇到action立即会阻塞
    rootPipelineType match {

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
