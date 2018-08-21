package io.github.interestinglab.waterdrop.output

class S3 extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("s3://", "s3a://", "s3n://"))
  }
}
