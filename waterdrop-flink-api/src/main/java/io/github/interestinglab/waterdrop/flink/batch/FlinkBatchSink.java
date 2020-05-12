package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.flink.BaseFlinkSink;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;

public interface FlinkBatchSink<IN, OUT> extends BaseFlinkSink {

    DataSink<OUT> outputBatch(FlinkEnvironment env, DataSet<IN> inDataSet);
}
