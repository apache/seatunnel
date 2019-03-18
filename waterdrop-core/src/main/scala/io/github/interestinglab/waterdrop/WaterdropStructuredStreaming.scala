package io.github.interestinglab.waterdrop

import java.io.File

import io.github.interestinglab.waterdrop.apis._
import io.github.interestinglab.waterdrop.config._
import io.github.interestinglab.waterdrop.filter.UdfRegister
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object WaterdropStructuredStreaming extends Logging {

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

        val appType = cmdArgs

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
                    Waterdrop.showConfigError(exception)
                  }
                  case false => {
                    Waterdrop.showFatalError(exception)
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

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    println("[INFO] loading SparkConf: ")
    val sparkConf = Waterdrop.createSparkConf(configBuilder)
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

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
      p.getDataset(sparkSession)
    })

    // let static input register as table for later use if needed
    Waterdrop.registerTempView(staticInputs, sparkSession)

    Waterdrop.showWaterdropAsciiLogo()

    var ds: Dataset[Row] = datasetList.get(0)
    for (f <- filters) {
      ds = f.process(sparkSession, ds)
    }

    var streamingQueryList = List[StreamingQuery]()

    for (output <- structuredStreamingOutputs) {
      val start = output.process(ds).start()
      streamingQueryList = streamingQueryList :+ start
    }

    for (streamingQuery <- streamingQueryList) {
      streamingQuery.awaitTermination()
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
