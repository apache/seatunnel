package io.github.interestinglab.waterdrop.spark.batch.transform

import io.github.interestinglab.waterdrop.apis.BaseTransform
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkBatchTransform extends BaseTransform[Dataset[Row], Dataset[Row], SparkBatchEnv]{

}
