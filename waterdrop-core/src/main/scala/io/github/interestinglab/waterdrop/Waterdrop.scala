package io.github.interestinglab.waterdrop

import java.io.File

import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.utils.CompressionUtils
import io.github.interestinglab.waterdrop.utils.AsciiArt
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Waterdrop extends Logging {

  def main(args: Array[String]) {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)
        val configFilePath = getConfigFilePath(cmdArgs)

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {
            Try(entrypoint(configFilePath)) match {
              case Success(_) => {}
              case Failure(exception) => {
                exception match {
                  case e: ConfigRuntimeException => showConfigError(e)
                  case e: Exception => showFatalError(e)
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

  private[waterdrop] def getConfigFilePath(cmdArgs: CommandLineArgs): String = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {
          // only keep filename in cluster mode
          new Path(cmdArgs.configFile).getName
        } else {
          cmdArgs.configFile
        }
      }
    }
  }

  private[waterdrop] def showWaterdropAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("Waterdrop")
  }

  private[waterdrop] def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
  }

  private[waterdrop] def showFatalError(throwable: Throwable): Unit = {
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
    println("[INFO] loading SparkConf: ")
    val sparkConf = createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    val staticInputs = configBuilder.createStaticInputs("batch")
    val streamingInputs = configBuilder.createStreamingInputs("batch")
    val filters = configBuilder.createFilters
    val outputs = configBuilder.createOutputs[BaseOutput]("batch")

    baseCheckConfig(staticInputs, streamingInputs, filters, outputs)

    if (streamingInputs.nonEmpty) {
      streamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, filters, outputs)
    } else {
      batchProcessing(sparkSession, configBuilder, staticInputs, filters, outputs)
    }
  }

  /**
   * Streaming Processing
   * */
  private def streamingProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    streamingInputs: List[BaseStreamingInput[Any]],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {

    val sparkConfig = configBuilder.getSparkConfigs
    val duration = sparkConfig.getLong("spark.streaming.batchDuration")
    val sparkConf = createSparkConf(configBuilder)
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))

    basePrepare(sparkSession, staticInputs, streamingInputs, filters, outputs)

    // let static input register as table for later use if needed
    registerTempView(staticInputs, sparkSession)
    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    streamingInputs(0).start(
      sparkSession,
      ssc,
      dataset => {

        var ds = dataset

        // Ignore empty schema dataset
        for (f <- filters) {
          if (ds.take(1).length > 0) {
            ds = f.process(sparkSession, ds)
          }
        }

        streamingInputs(0).beforeOutput

        outputs.foreach(p => {
          p.process(ds)
        })

        streamingInputs(0).afterOutput

      }
    )

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

    basePrepare(sparkSession, staticInputs, filters, outputs)

    // let static input register as table for later use if needed
    registerTempView(staticInputs, sparkSession)

    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    if (staticInputs.nonEmpty) {
      var ds = staticInputs.head.getDataset(sparkSession)

      for (f <- filters) {
        if (ds.take(1).length > 0) {
          ds = f.process(sparkSession, ds)
        }
      }
      outputs.foreach(p => {
        p.process(ds)
      })

    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }
  }

  private[waterdrop] def basePrepare(sparkSession: SparkSession, plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
      }
    }
  }

  private[waterdrop] def baseCheckConfig(plugins: List[Plugin]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
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
    }
    deployModeCheck()
  }

  private[waterdrop] def registerTempView(staticInputs: List[BaseStaticInput], sparkSession: SparkSession): Unit = {
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

  private[waterdrop] def deployModeCheck(): Unit = {
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
  }

  private[waterdrop] def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }
}
