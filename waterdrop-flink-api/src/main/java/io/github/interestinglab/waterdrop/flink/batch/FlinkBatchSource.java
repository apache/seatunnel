package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.apis.BaseSource;
import org.apache.flink.api.java.DataSet;

public interface FlinkBatchSource<T> extends BaseSource<DataSet<T>, FlinkBatchEnvironment> {

    @Override
    DataSet<T> getData(FlinkBatchEnvironment env);
}
