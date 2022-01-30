package org.apache.seatunnel.spark.batch;

import org.apache.seatunnel.spark.BaseSparkSource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class SparkBatchSource extends BaseSparkSource<Dataset<Row>> {
}
