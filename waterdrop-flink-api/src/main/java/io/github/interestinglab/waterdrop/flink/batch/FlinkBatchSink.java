package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.flink.BaseFlinkSink;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:19
 * @description
 */
public interface FlinkBatchSink<IN, OUT> extends BaseFlinkSink {

    DataSink<OUT> outputBatch(FlinkEnvironment env, DataSet<IN> inDataSet);
}
