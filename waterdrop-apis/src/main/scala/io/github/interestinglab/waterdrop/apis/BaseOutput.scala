package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.DataFrame

abstract class BaseOutput extends Plugin {

  def process(df: DataFrame)
}
