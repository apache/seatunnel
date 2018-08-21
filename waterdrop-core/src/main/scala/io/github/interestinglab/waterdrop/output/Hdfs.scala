package io.github.interestinglab.waterdrop.output

class Hdfs extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("hdfs://"))
  }
}
