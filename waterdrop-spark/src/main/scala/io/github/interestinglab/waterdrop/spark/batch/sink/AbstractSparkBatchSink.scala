package io.github.interestinglab.waterdrop.spark.batch.sink

import io.github.interestinglab.waterdrop.apis.BaseSink
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkBatchSink extends BaseSink[Dataset[Row], Void, SparkBatchEnv]{

}
