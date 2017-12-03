package org.interestinglab.waterdrop.config

import java.nio.file.{Path, Paths}

object Common {

  val ALLOWED_MODES = List("local", "local\\[\\d+\\]", "yarn-client", "yarn-cluster")

  // local / local[x] / yarn-client / yarn-cluster
  var mode: Option[String] = None

  /**
   * Root dir varies between different spark master and deploy mode,
   * it also varies between relative and absolute path.
   * */
  def appRootDir(): Path = {
    // if running on local
    // if running on yarn-client(client driver, executor)
    // if running on yarn-cluster(cluster driver, executor)
    // if running on mesos ??

    // TODO: mesos
    mode match {
      case Some(m) if m.startsWith("local") => {

        Paths.get(Common.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      }
      case Some("yarn-client") => {

        Paths.get(Common.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      }

      case Some("yarn-cluster") => {

        Paths.get("")
      }
    }
  }

  /**
   * Plugin Root Dir
   * */
  def pluginRootDir(): Path = {

    Paths.get(appRootDir().toString, "plugins")
  }

  /**
   * Get specific plugin dir
   * */
  def pluginDir(pluginName: String): Path = {

    Paths.get(pluginRootDir().toString, pluginName)
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
