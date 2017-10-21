package org.interestinglab.waterdrop.output

import com.typesafe.config.Config

class File(config: Config) extends FileOutputBase(config) {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("file://"))
  }
}
