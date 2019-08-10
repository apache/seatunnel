package io.github.interestinglab.waterdrop.spark.batch.sink
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

class Stdout extends AbstractSparkBatchSink {
  override def output(data: Dataset[Row], env: SparkBatchEnv): Unit = {
    data.show()
  }
}
