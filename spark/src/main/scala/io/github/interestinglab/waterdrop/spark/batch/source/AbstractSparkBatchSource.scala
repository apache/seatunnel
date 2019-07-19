package io.github.interestinglab.waterdrop.spark.batch.source

import io.github.interestinglab.waterdrop.apis.BaseSource
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkBatchSource extends BaseSource[Dataset[Row], SparkBatchEnv]{

}
