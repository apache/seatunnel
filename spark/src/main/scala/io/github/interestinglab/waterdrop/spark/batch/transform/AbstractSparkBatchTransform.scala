package io.github.interestinglab.waterdrop.spark.batch.transform

import com.sun.javafx.geom.transform.BaseTransform
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkBatchTransform extends BaseTransform[Dataset[Row], Dataset[Row], SparkBatchEnv]{

}
