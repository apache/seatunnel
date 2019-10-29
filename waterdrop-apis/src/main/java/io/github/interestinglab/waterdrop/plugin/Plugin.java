package io.github.interestinglab.waterdrop.plugin;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.common.config.CheckResult;

import java.io.Serializable;

/**
 * @author mr_xiong
 * @date 2019-05-28
 * @description
 */
public interface Plugin extends Serializable {
    String RESULT_TABLE_NAME = "result_table_name";
    String SOURCE_TABLE_NAME = "source_table_name";

    void setConfig(Config config);

    Config getConfig();

    CheckResult checkConfig();

    void prepare();

}
