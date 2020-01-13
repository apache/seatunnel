package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.flink.BaseFlinkSource;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;

public interface FlinkBatchSource<T> extends BaseFlinkSource {

    DataSet<T> getData(FlinkEnvironment env);
}
