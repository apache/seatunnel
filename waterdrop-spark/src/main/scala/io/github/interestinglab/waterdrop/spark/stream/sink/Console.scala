package io.github.interestinglab.waterdrop.spark.stream.sink
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingEnv
import org.apache.spark.sql.{Dataset, Row}

class Console extends AbstractSparkStreamingSink {
  override def output(data: Dataset[Row], env: SparkStreamingEnv): Unit = {
    data.show()
  }
}
