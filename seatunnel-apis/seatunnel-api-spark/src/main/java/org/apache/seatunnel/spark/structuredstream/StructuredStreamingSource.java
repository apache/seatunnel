package org.apache.seatunnel.spark.structuredstream;

import org.apache.seatunnel.spark.BaseSparkSource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class StructuredStreamingSource extends BaseSparkSource<Dataset<Row>> {
}
