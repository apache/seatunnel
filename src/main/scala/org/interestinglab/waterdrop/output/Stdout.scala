package org.interestinglab.waterdrop.output

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class Stdout(config: Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = (true, "")

  override def process(df: DataFrame): Unit = {

    if (config.hasPath("limit")) {
      df.show(config.getInt("limit"), false)
    } else {
      val num = 20
      df.show(num, false)
    }
  }
}
