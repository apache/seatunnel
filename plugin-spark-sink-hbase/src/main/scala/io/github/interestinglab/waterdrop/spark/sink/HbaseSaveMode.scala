package io.github.interestinglab.waterdrop.spark.sink

object HbaseSaveMode extends Enumeration {

  type HbaseSaveMode = Value

  val Append, Overwrite = Value
}