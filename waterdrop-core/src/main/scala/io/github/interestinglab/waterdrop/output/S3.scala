package io.github.interestinglab.waterdrop.output

import com.typesafe.config.Config

class S3(config: Config) extends FileOutputBase(config) {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("s3://", "s3a://", "s3n://"))
  }
}
