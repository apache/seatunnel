package io.github.interestinglab.waterdrop.spark.batch

import io.github.interestinglab.waterdrop.spark.BaseSparkSource
import org.apache.spark.sql.{Dataset, Row}

trait SparkBatchSource extends BaseSparkSource[Dataset[Row]]{

}
