package io.github.interestinglab.waterdrop.spark.sink

object ElasticsearchSaveMode extends Enumeration{
  type HbaseSaveMode = Value

  val Append, Overwrite = Value

}
