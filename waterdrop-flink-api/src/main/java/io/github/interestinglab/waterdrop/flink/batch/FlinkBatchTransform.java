package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.flink.BaseFlinkTransform;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:23
 * @description
 */
public interface FlinkBatchTransform<IN,OUT> extends BaseFlinkTransform {

    DataSet<OUT> processBatch(FlinkEnvironment env, DataSet<IN> data);

}
