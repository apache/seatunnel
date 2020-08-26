package io.github.interestinglab.waterdrop.plugin;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckResult;

import java.io.Serializable;

public interface Plugin<T> extends Serializable {
    String RESULT_TABLE_NAME = "result_table_name";
    String SOURCE_TABLE_NAME = "source_table_name";

    void setConfig(Config config);

    Config getConfig();

    CheckResult checkConfig();

    void prepare(T prepareEnv);

}
