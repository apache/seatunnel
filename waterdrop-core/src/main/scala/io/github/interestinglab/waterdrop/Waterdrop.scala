package io.github.interestinglab.waterdrop

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.pipelines.Pipeline.{Batch, PipelineType, Streaming, Unknown}
import io.github.interestinglab.waterdrop.pipelines.{Pipeline, PipelineBuilder, PipelineRunner}
import io.github.interestinglab.waterdrop.utils.CompressionUtils
import io.github.interestinglab.waterdrop.utils.AsciiArt
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
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

            Try(entrypoint(configFilePath)) match {
              case Success(_) => {}
              case Failure(exception) => {
                exception.isInstanceOf[ConfigRuntimeException] match {
                  case true => {
                    showConfigError(exception)
                  }
                  case false => {
                    showFatalError(exception)
                  }
                }
              }
            }
          }
        }
      }
      case None =>
      // CommandLineUtils.parser.showUsageAsError()
      // CommandLineUtils.parser.terminate(Right(()))
    }
  }

  private def showWaterdropAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Waterdrop")
  }

  private def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
  }

  private def showFatalError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Fatal Error, \n")
    println(
      "Please contact garygaowork@gmail.com or issue a bug in https://github.com/InterestingLab/waterdrop/issues\n")
    println("Reason: " + errorMsg + "\n")
    println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable))
    println("\n===============================================================================\n\n\n")
  }

  private def entrypoint(configFile: String): Unit = {

    val rootConfig = ConfigFactory.parseFile(new File(configFile))

    val (rootPipeline, rootPipelineType, _) = PipelineBuilder.recursiveBuilder(rootConfig, "ROOT_PIPELINE")

    rootPipelineType match {
      case Unknown => {
        throw new ConfigRuntimeException("Cannot not detect pipeline type, please check your config")
      }
      case _ => {}
    }

    PipelineBuilder.validatePipeline(rootPipeline)

    PipelineBuilder.checkConfigRecursively(rootPipeline)

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

    process(rootConfig, rootPipeline, rootPipelineType)
  }

  private def process(rootConfig: Config, rootPipeline: Pipeline, rootPipelineType: PipelineType): Unit = {
    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(rootConfig.getConfig("spark"))
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    PipelineRunner.preparePipelineRecursively(sparkSession, rootPipeline)

    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    rootPipelineType match {

      case Streaming => {

        val sparkConfig = rootConfig.getConfig("spark")
        val duration = sparkConfig.getLong("spark.streaming.batchDuration")
        val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))
        PipelineRunner.pipelineRunnerForStreaming(rootPipeline, sparkSession, ssc)

        ssc.start()
        ssc.awaitTermination()
      }
      case Batch => {
        PipelineRunner.pipelineRunnerForBatch(rootPipeline, sparkSession)
      }
    }
  }

  private def createSparkConf(sparkConfig: Config): SparkConf = {
    val sparkConf = new SparkConf()

    sparkConfig
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        logInfo("spark config - key: %s, value: %s".format(entry.getKey, value))
        sparkConf.set(key, value)
      })

    sparkConf
  }
}
