package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.flink.BaseFlinkTransform;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;

public interface FlinkBatchTransform<IN,OUT> extends BaseFlinkTransform {

    DataSet<OUT> processBatch(FlinkEnvironment env, DataSet<IN> data);

}
