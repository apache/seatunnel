package io.github.interestinglab.waterdrop.flink.batch;

import io.github.interestinglab.waterdrop.apis.BaseTransform;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import org.apache.flink.api.java.DataSet;

/**
 * @author mr_xiong
 * @date 2019-08-24 16:23
 * @description
 */
public interface FlinkBatchTransform<IN,OUT> extends BaseTransform {

    DataSet<OUT> processBatch(DataSet<IN> data, FlinkEnvironment env);

}
