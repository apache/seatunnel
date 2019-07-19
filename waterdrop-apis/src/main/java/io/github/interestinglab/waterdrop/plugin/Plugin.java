package io.github.interestinglab.waterdrop.plugin;

import com.typesafe.config.Config;

import java.io.Serializable;

/**
 * @author mr_xiong
 * @date 2019-05-28
 * @description
 */
public interface Plugin extends Serializable {
    void setConfig(Config config);
    Config getConfig();
    CheckResult checkConfig();
    void prepare();
}
