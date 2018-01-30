package io.github.interestinglab.waterdrop

import java.io.File

import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseInput, BaseOutput}
import io.github.interestinglab.waterdrop.config.{CommandLineArgs, CommandLineUtils, Common, ConfigBuilder}
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.utils.CompressionUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
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

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(cmdArgs.configFile)
            println("config OK !")
          }
          case false => {

            entrypoint(cmdArgs.configFile)
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
    val sparkConfig = configBuilder.getSparkConfigs
    val inputs = configBuilder.createInputs
    val outputs = configBuilder.createOutputs
    val filters = configBuilder.createFilters

    var configValid = true
    val plugins = inputs ::: filters ::: outputs
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

    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val duration = sparkConfig.getLong("spark.streaming.batchDuration")
    val ssc = new StreamingContext(sparkConf, Seconds(duration))
    val sparkSession = SparkSession.builder.config(ssc.sparkContext.getConf).getOrCreate()

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

    process(sparkSession, ssc, inputs, filters, outputs)
  }

  private def process(
    sparkSession: SparkSession,
    ssc: StreamingContext,
    inputs: List[BaseInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    for (i <- inputs) {
      i.prepare(sparkSession, ssc)
    }

    for (o <- outputs) {
      o.prepare(sparkSession, ssc)
    }

    for (f <- filters) {
      f.prepare(sparkSession, ssc)
    }

    val dstreamList = inputs.map(p => {
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

      val schema = StructType(Array(StructField("raw_message", StringType)))
      var df = spark.createDataFrame(rowsRDD, schema)

      for (f <- filters) {
        df = f.process(spark, df)
      }

      inputs.foreach(p => {
        p.beforeOutput
      })

      outputs.foreach(p => {
        p.process(df)
      })

      inputs.foreach(p => {
        p.afterOutput
      })

    }

    ssc.start()
    ssc.awaitTermination()
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
