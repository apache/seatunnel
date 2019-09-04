package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.apis.BaseSink;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:19
 * @description
 */
public interface FlinkBatchSink<IN,OUT> extends BaseSink<DataSet<IN>, DataSink<OUT>,FlinkBatchEnvironment> {
    @Override
    DataSink<OUT> output(DataSet<IN> inDataSet, FlinkBatchEnvironment env);
}
