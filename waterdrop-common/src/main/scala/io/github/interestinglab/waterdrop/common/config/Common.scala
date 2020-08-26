package io.github.interestinglab.waterdrop.common.config

import java.nio.file.{Path, Paths}

object Common {

  val allowedModes = List("client", "cluster")

  private var mode: Option[String] = None

  def isModeAllowed(mode: String): Boolean = {

    allowedModes.foldRight(false)((m, lastResult) => {
      lastResult match {
        case true  => true
        case false => mode.toLowerCase.equals(m)
      }
    })
  }

  /**
    * Set mode. return false in case of failure
    * */
  def setDeployMode(mode: String): Boolean = {

    isModeAllowed(mode) match {
      case true => {
        this.mode = Some(mode)
        true
      }
      case false => false
    }
  }

  def getDeployMode: Option[String] = {
    this.mode
  }

  /**
    * Root dir varies between different spark master and deploy mode,
    * it also varies between relative and absolute path.
    * When running waterdrop in --master local, you can put plugins related files in $project_dir/plugins,
    * then these files will be automatically copied to $project_dir/waterdrop-core/target and token in effect if you start waterdrop in IDE tools such as IDEA.
    * When running waterdrop in --master yarn or --master mesos, you can put plugins related files in plugins dir.
    * */
  def appRootDir: Path = {

    this.mode match {
      case Some("client") => {

        val path =
          Common.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
        Paths.get(path).getParent.getParent.getParent
      }

      case Some("cluster") => {

        Paths.get("")
      }
    }
  }

  /**
    * Plugin Root Dir
    * */
  def pluginRootDir: Path = {

    Paths.get(appRootDir.toString, "plugins")
  }

  /**
    * Get specific plugin dir
    * */
  def pluginDir(pluginName: String): Path = {

    Paths.get(pluginRootDir.toString, pluginName)
  }

  /**
    * Get files dir of specific plugin
    * */
  def pluginFilesDir(pluginName: String): Path = {

    Paths.get(pluginDir(pluginName).toString, "files")
  }

  /**
    * Get lib dir of specific plugin
    * */
  def pluginLibDir(pluginName: String): Path = {

    Paths.get(pluginDir(pluginName).toString, "lib")
  }
}
