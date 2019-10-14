package io.github.interestinglab.waterdrop.common.config;

import com.typesafe.config.waterdrop.Config;

/**
 * @author mr_xiong
 * @date 2019-09-25 16:00
 * @description
 */
public class CheckConfigUtil {

    public static CheckResult check(Config config, String... params) {
        for (String param : params) {
            if (!config.hasPath(param) || config.getString(param) == null || config.getString(param).trim().isEmpty()) {
                return new CheckResult(false, "please specify [" + param + "] as non-empty string");
            }
        }
        return new CheckResult(true,"");
    }
}
