package io.github.interestinglab.waterdrop.spark.structuredstream

import io.github.interestinglab.waterdrop.spark.BaseSparkSource
import org.apache.spark.sql.{Dataset, Row}

trait StructuredStreamingSource extends BaseSparkSource[Dataset[Row]]{

}
