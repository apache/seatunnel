package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.common.config.{CheckResult, ConfigRuntimeException}
import io.github.interestinglab.waterdrop.config.{ConfigBuilder, _}
import io.github.interestinglab.waterdrop.env.RuntimeEnv
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import io.github.interestinglab.waterdrop.plugin.Plugin
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.utils.AsciiArtUtils

import scala.util.{Failure, Success, Try}

object Waterdrop {

  val engine = "spark"
  def main(args: Array[String]) {

    CommandLineUtils.sparkParser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)
        val configFilePath = getConfigFilePath(cmdArgs,engine)

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {
            Try(entrypoint(configFilePath,engine)) match {
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
      // CommandLineUtils.sparkParser.showUsageAsError()
      // CommandLineUtils.sparkParser.terminate(Right(()))
    }
  }

  private[waterdrop] def getConfigFilePath(cmdArgs: CommandLineArgs, engine: String): String = {
    engine match {
      case "flink" => cmdArgs.configFile
      case "spark" => {
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
    }
  }

  private def entrypoint(configFile: String, engine: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile, engine)
    val (sources, isStreaming) = configBuilder.createSources
    val transforms = configBuilder.createTransforms
    val sinks = configBuilder.createSinks
    val (execution, env) = configBuilder.createExecution(isStreaming)
    baseCheckConfig(sources, transforms, sinks)
    prepare(env.asInstanceOf[SparkEnvironment], sources, transforms, sinks)
    showWaterdropAsciiLogo()

    execution.start(sources.asJava, transforms.asJava, sinks.asJava);
  }

  private[waterdrop] def showWaterdropAsciiLogo(): Unit = {
    AsciiArtUtils.printAsciiArt("Waterdrop")
  }

  private[waterdrop] def baseCheckConfig(plugins: scala.List[Plugin[_]]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        val checkResult = Try(p.checkConfig) match {
          case Success(info) => {
            info
          }
          case Failure(exception) => new CheckResult(false, exception.getMessage)
        }

        if (!checkResult.isSuccess) {
          configValid = false
          printf("Plugin[%s] contains invalid config, error: %s\n", p.getClass.getName, checkResult.getMsg)
        }
      }

      if (!configValid) {
        System.exit(-1) // invalid configuration
      }
    }
    // deployModeCheck()
  }

  private def prepare(env: SparkEnvironment, plugins: scala.List[Plugin[RuntimeEnv]]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(env)
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
