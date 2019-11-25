package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.config.{ConfigBuilder, _}

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
            Try(Waterdrop.entrypoint(configFilePath,engine)) match {
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


}
