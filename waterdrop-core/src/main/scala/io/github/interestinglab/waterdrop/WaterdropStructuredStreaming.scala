package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object WaterdropStructuredStreaming extends Logging {

  def main(args: Array[String]) {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)
        val configFilePath = Waterdrop.getConfigFilePath(cmdArgs)

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
                  case e @ (_: ConfigRuntimeException | _: UserRuntimeException) => Waterdrop.showConfigError(e)
                  case e: OffsetOutOfRangeException => showKnownError("Please remove checkpoint dir.", e)
                  case e: Exception => Waterdrop.showFatalError(e)
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

  private def showKnownError(str: String, throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Known error:\n")
    println(str + "\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")

  }

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    println("[INFO] loading SparkConf: ")
    val sparkConf = Waterdrop.createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    val staticInputs = configBuilder.createStaticInputs("structuredstreaming")
    val streamingInputs = configBuilder.createStructuredStreamingInputs("structuredstreaming")
    val filters = configBuilder.createFilters
    val outputs = configBuilder.createOutputs[BaseStructuredStreamingOutputIntra]("structuredstreaming")
    Waterdrop.baseCheckConfig(staticInputs, streamingInputs, filters, outputs)
    structuredStreamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, filters, outputs)

  }

  /**
   * Structured Streaming Processing
   * */
  private def structuredStreamingProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    structuredStreamingInputs: List[BaseStructuredStreamingInput],
    filters: List[BaseFilter],
    structuredStreamingOutputs: List[BaseStructuredStreamingOutputIntra]): Unit = {

    Waterdrop.basePrepare(sparkSession, staticInputs, structuredStreamingInputs, filters, structuredStreamingOutputs)

    val datasetList = structuredStreamingInputs.map(p => {
      val ds = p.getDataset(sparkSession)
      registerInputTempView(p, ds)
      ds
    })

    // let static input register as table for later use if needed
    Waterdrop.registerInputTempView(staticInputs, sparkSession)

    Waterdrop.showWaterdropAsciiLogo()

    var ds: Dataset[Row] = datasetList.get(0)
    for (f <- filters) {
      ds = Waterdrop.filterProcess(sparkSession, f, ds)
      Waterdrop.registerStreamingFilterTempView(f, ds)
    }

    var streamingQueryList = List[StreamingQuery]()

    for (output <- structuredStreamingOutputs) {
      val start = structuredStreamingOutputProcess(sparkSession, output, ds).start()
      streamingQueryList = streamingQueryList :+ start
    }

    for (streamingQuery <- streamingQueryList) {
      streamingQuery.awaitTermination()
    }
  }

  private def registerInputTempView(input: BaseStructuredStreamingInput, ds: Dataset[Row]): Unit = {
    val config = input.getConfig()
    config.hasPath("table_name") || config.hasPath("result_table_name") match {
      case true => {
        val tableName = config.hasPath("table_name") match {
          case true => {
            @deprecated
            val oldTableName = config.getString("table_name")
            oldTableName
          }
          case false => config.getString("result_table_name")
        }
        Waterdrop.registerStreamingTempView(tableName, ds)
      }

      case false => {
        throw new ConfigRuntimeException(
          "Plugin[" + input.name + "] must be registered as dataset/table, please set \"result_table_name\" config")

      }
    }
  }

  private def structuredStreamingOutputProcess(
    sparkSession: SparkSession,
    output: BaseStructuredStreamingOutputIntra,
    ds: Dataset[Row]): DataStreamWriter[Row] = {
    val config = output.getConfig()
    val fromDs = config.hasPath("source_table_name") match {
      case true => {
        val sourceTableName = config.getString("source_table_name")
        sparkSession.read.table(sourceTableName)
      }
      case false => ds
    }

    output.process(fromDs)
  }

}
