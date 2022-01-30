package org.apache.seatunnel.spark.structuredstream;

import org.apache.seatunnel.spark.BaseSparkSink;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class StructuredStreamingSink extends BaseSparkSink<DataStreamWriter<Row>> {
}
