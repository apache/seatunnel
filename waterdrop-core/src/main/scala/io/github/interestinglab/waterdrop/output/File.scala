package io.github.interestinglab.waterdrop.output

class File extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("file://"))
  }
}
