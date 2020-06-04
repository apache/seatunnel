package org.apache.spark.sql.execution.datasources.jdbc2

object JDBCSaveMode extends Enumeration {

  type JDBCSaveMode = Value

  val Append, Overwrite, ErrorIfExists, Ignore, Update = Value

}
