package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.config.{ConfigBuilder, _}
import io.github.interestinglab.waterdrop.plugin.Plugin
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object WaterdropFlink {

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
                  case e: ConfigRuntimeException => Waterdrop.showConfigError(e)
                  case e: Exception              => Waterdrop.showFatalError(e)
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

    val configBuilder = new ConfigBuilder(configFile, "flink")
    val (sources, isStreaming) = configBuilder.createSources
    println(isStreaming)
    val transforms = configBuilder.createTransforms
    val sinks = configBuilder.createSinks

    val (runtimeEnv, execution) = configBuilder.createExecution

    runtimeEnv.setConfig(configBuilder.config)
    runtimeEnv.prepare(isStreaming)

    Waterdrop.prepare(sources, transforms, sinks)
    execution.start(sources.asJava, transforms.asJava, sinks.asJava);

  }


}
