package io.github.interestinglab.waterdrop

import java.io.File

import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseOutput, BaseStaticInput, BaseStreamingInput}
import io.github.interestinglab.waterdrop.config.{CommandLineArgs, CommandLineUtils, Common, ConfigBuilder}
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.utils.CompressionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Waterdrop extends Logging {

  def main(args: Array[String]) {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)

        val configFilePath = Common.getDeployMode match {
          case Some(m) => {
            if (m.equals("cluster")) {
              // only keep filename in cluster mode
              new Path(cmdArgs.configFile).getName
            } else {
              cmdArgs.configFile
            }
          }
        }

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {

            entrypoint(configFilePath)
          }
        }
      }
      case None =>
      // CommandLineUtils.parser.showUsageAsError()
      // CommandLineUtils.parser.terminate(Right(()))
    }
  }

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    val staticInputs = configBuilder.createStaticInputs
    val streamingInputs = configBuilder.createStreamingInputs
    val outputs = configBuilder.createOutputs
    val filters = configBuilder.createFilters

    var configValid = true
    val plugins = staticInputs ::: streamingInputs ::: filters ::: outputs
    for (p <- plugins) {
      val (isValid, msg) = Try(p.checkConfig) match {
        case Success(info) => {
          val (ret, message) = info
          (ret, message)
        }
        case Failure(exception) => (false, exception.getMessage)
      }

      if (!isValid) {
        configValid = false
        printf("Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
      }
    }

    if (!configValid) {
      System.exit(-1) // invalid configuration
    }

    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {

          logInfo("preparing cluster mode work dir files...")

          // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
          val workDir = new File(".")
          logWarning("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

          workDir.listFiles().foreach(f => logWarning("\t list file: " + f.getAbsolutePath))

          // decompress plugin dir
          val compressedFile = new File("plugins.tar.gz")

          Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
            case Success(tempFile) => {
              Try(CompressionUtils.unTar(tempFile, workDir)) match {
                case Success(_) => logInfo("succeeded to decompress plugins.tar.gz")
                case Failure(ex) => {
                  logError("failed to decompress plugins.tar.gz", ex)
                  sys.exit(-1)
                }
              }

            }
            case Failure(ex) => {
              logError("failed to decompress plugins.tar.gz", ex)
              sys.exit(-1)
            }
          }
        }
      }
    }

    process(configBuilder, staticInputs, streamingInputs, filters, outputs)
  }

  private def process(
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    streamingInputs: List[BaseStreamingInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    // [done] build static input from config builder, register static input dataset name.
    // [done] input / dataset name register / primary / secondary input  ---> 默认先取第一个
    // [done]（1）当streaming input 有 0 个的时候，（2）当streaming input 有 1 个的时候，（3）当streaming input 有 > 1 个的时候，
    // [done] 区分 streaming, batch 流程 ...
    // [done] prepare(ssc = ???), ssc ???

    streamingInputs.size match {
      case 0 => {
        batchProcessing(sparkSession, configBuilder, staticInputs, filters, outputs)
      }
      case _ => {
        streamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, filters, outputs)
      }
    }
  }

  /**
   * Streaming Processing
   * */
  private def streamingProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    streamingInputs: List[BaseStreamingInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    // TODO: static input, register static input dataset name.

    val sparkConfig = configBuilder.getSparkConfigs
    val duration = sparkConfig.getLong("spark.streaming.batchDuration")
    val sparkConf = createSparkConf(configBuilder)
    val ssc = new StreamingContext(sparkConf, Seconds(duration))

    for (i <- staticInputs) {
      i.prepare(sparkSession)
    }

    for (i <- streamingInputs) {
      i.prepare(sparkSession)
    }

    for (o <- outputs) {
      o.prepare(sparkSession)
    }

    for (f <- filters) {
      f.prepare(sparkSession)
    }

    val dstreamList = streamingInputs.map(p => {
      p.getDStream(ssc)
    })

    val unionedDStream = dstreamList.reduce((d1, d2) => {
      d1.union(d2)
    })

    val dStream = unionedDStream.mapPartitions { partitions =>
      val strIterator = partitions.map(r => r._2)
      val strList = strIterator.toList
      strList.iterator
    }

    dStream.foreachRDD { strRDD =>
      val rowsRDD = strRDD.mapPartitions { partitions =>
        val row = partitions.map(Row(_))
        val rows = row.toList
        rows.iterator
      }

      val spark = SparkSession.builder.config(rowsRDD.sparkContext.getConf).getOrCreate()
      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

      val schema = StructType(Array(StructField("raw_message", StringType)))
      val encoder = RowEncoder(schema)
      var ds = spark.createDataset(rowsRDD)(encoder)

      for (f <- filters) {
        ds = f.process(spark, ds)
      }

      streamingInputs.foreach(p => {
        p.beforeOutput
      })

      outputs.foreach(p => {
        p.process(ds)
      })

      streamingInputs.foreach(p => {
        p.afterOutput
      })

    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Batch Processing
   * */
  private def batchProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    for (i <- staticInputs) {
      i.prepare(sparkSession)
    }

    for (o <- outputs) {
      o.prepare(sparkSession)
    }

    for (f <- filters) {
      f.prepare(sparkSession)
    }

    var ds = staticInputs(0).createDataset(sparkSession)
    for (f <- filters) {
      ds = f.process(sparkSession, ds)
    }

    outputs.foreach(p => {
      p.process(ds)
    })
  }

  private def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }
}
