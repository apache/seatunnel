package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.config.{ConfigBuilder, _}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import io.github.interestinglab.waterdrop.apis.{
  BaseSink,
  BaseSource,
  BaseTransform
}
import io.github.interestinglab.waterdrop.env.{Execution, RuntimeEnv}
import io.github.interestinglab.waterdrop.plugin.Plugin
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchExecution
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingExecution

import scala.util.{Failure, Success, Try}

object Waterdrop {

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
                  case e: Exception              => showFatalError(e)
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

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)

    val staticInputs = configBuilder.createStaticInputs

    val filters = configBuilder.createFilters
    val outputs = configBuilder.createOutputs

    val runtimeEnv =
      configBuilder.createRuntimeEnv

    val execution = new SparkBatchExecution(
      runtimeEnv.asInstanceOf[SparkEnvironment])
      .asInstanceOf[Execution[BaseSource[_, _],
                              BaseTransform[_, _, _],
                              BaseSink[_, _, _]]]

    runtimeEnv.setConfig(configBuilder.config)

    prepare(runtimeEnv, staticInputs, filters, outputs)
    execution.start(staticInputs.asJava, filters.asJava, outputs.asJava);

  }

  private[waterdrop] def prepare(runtimeEnv: RuntimeEnv,
                                 plugins: scala.List[Plugin]*): Unit = {
    runtimeEnv.prepare()
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare()
      }
    }
  }

  private[waterdrop] def showConfigError(throwable: Throwable): Unit = {
    println(
      "\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println(
      "\n===============================================================================\n\n\n")
  }

  private[waterdrop] def showFatalError(throwable: Throwable): Unit = {
    println(
      "\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Fatal Error, \n")
    println(
      "Please contact garygaowork@gmail.com or issue a bug in https://github.com/InterestingLab/waterdrop/issues\n")
    println("Reason: " + errorMsg + "\n")
    println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable))
    println(
      "\n===============================================================================\n\n\n")
  }

}
