package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.apis.BaseSource;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;

public interface FlinkBatchSource<T> extends BaseSource<FlinkEnvironment> {

    DataSet<T> getData(FlinkEnvironment env);
}
