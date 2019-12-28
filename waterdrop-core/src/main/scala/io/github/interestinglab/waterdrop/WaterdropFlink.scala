package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.Waterdrop.{baseCheckConfig, prepare, showWaterdropAsciiLogo}
import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.config.{ConfigBuilder, _}
import io.github.interestinglab.waterdrop.env.RuntimeEnv
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment
import io.github.interestinglab.waterdrop.plugin.Plugin
import scala.collection.JavaConverters._

import scala.util.{Failure, Success, Try}

object WaterdropFlink {
  val engine = "flink"

  def main(args: Array[String]) {

    CommandLineUtils.flinkParser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)
        val configFilePath = Waterdrop.getConfigFilePath(cmdArgs,engine)

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
                  case e: ConfigRuntimeException => Waterdrop.showConfigError(e)
                  case e: Exception              => Waterdrop.showFatalError(e)
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

  private def entrypoint(configFile: String, engine: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile, engine)
    val (sources, isStreaming) = configBuilder.createSources
    val transforms = configBuilder.createTransforms
    val sinks = configBuilder.createSinks
    val (execution, env) = configBuilder.createExecution(isStreaming)
    baseCheckConfig(sources, transforms, sinks)
    prepare(env.asInstanceOf[FlinkEnvironment], sources, transforms, sinks)
    showWaterdropAsciiLogo()

    execution.start(sources.asJava, transforms.asJava, sinks.asJava);
  }

  private def prepare(env: FlinkEnvironment, plugins: scala.List[Plugin[RuntimeEnv]]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(env)
      }
    }
  }

}
