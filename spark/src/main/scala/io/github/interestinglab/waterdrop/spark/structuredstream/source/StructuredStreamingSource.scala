package io.github.interestinglab.waterdrop.spark.structuredstream.source

import io.github.interestinglab.waterdrop.apis.BaseSource
import io.github.interestinglab.waterdrop.spark.structuredstream.StructuredStreamingEnv
import org.apache.spark.sql.{Dataset, Row}

trait StructuredStreamingSource extends BaseSource[Dataset[Row], StructuredStreamingEnv]{

}
