package io.github.interestinglab.waterdrop.flink;

import io.github.interestinglab.waterdrop.apis.BaseTransform;


public interface BaseFlinkTransform extends BaseTransform<FlinkEnvironment> {

    default void registerFunction(FlinkEnvironment flinkEnvironment){

    }

}
