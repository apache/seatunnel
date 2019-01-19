package io.github.interestinglab.waterdrop

import java.io.File

import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.utils.CompressionUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


object Application extends Logging {

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

    val configBuilder = new ConfigBuilder(configFile)
    val structuredStreamingInputs = configBuilder.createStructuredStreamingInputs
    val structuredStreamingOutputs = configBuilder.createStructuredStreamingOutputs
    val filters = configBuilder.createFilters

    var configValid = true
    val plugins = structuredStreamingInputs ::: structuredStreamingOutputs ::: filters
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

    process(configBuilder, structuredStreamingInputs, filters, structuredStreamingOutputs)
  }

  private def process(
                       configBuilder: ConfigBuilder,
                       structuredStreamingInputs: List[BaseStructuredStreamingInput],
                       filters: List[BaseFilter],
                       structuredStreamingOutputs: List[BaseStructuredStreamingOutput]): Unit = {

    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

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

  private def streamingProcessing(
                                   sparkSession: SparkSession,
                                   configBuilder: ConfigBuilder,
                                   structuredStreamingInputs: List[BaseStructuredStreamingInput],
                                   filters: List[BaseFilter],
                                   structuredStreamingOutputs: List[BaseStructuredStreamingOutput]): Unit = {


    basePrepare(sparkSession, structuredStreamingInputs, filters, structuredStreamingOutputs)

    val datasetList = structuredStreamingInputs.map(p => {
      p.getDataset(sparkSession)
    })

    var ds: Dataset[Row] = datasetList.get(0)
    for (f <- filters) {
      ds = f.process(sparkSession, ds)
    }
    val output = structuredStreamingOutputs.get(0)
    val start = output.process(ds).start()
    start.awaitTermination()
  }

  private def basePrepare(
                           sparkSession: SparkSession,
                           structuredStreamingInputs: List[BaseStructuredStreamingInput],
                           filters: List[BaseFilter],
                           structuredStreamingOutputs: List[BaseStructuredStreamingOutput]): Unit = {

    for (i <- structuredStreamingInputs) {
      i.prepare(sparkSession)
    }

    for (o <- structuredStreamingOutputs) {
      o.prepare(sparkSession)
    }

    for (f <- filters) {
      f.prepare(sparkSession)
    }
  }

  private def listener(sparkSession: SparkSession): Unit = {
    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        //do something
      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        //do listener
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        //do something
      }
    })
  }
}
