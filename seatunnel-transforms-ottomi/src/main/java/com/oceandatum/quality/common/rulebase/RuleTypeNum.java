package com.oceandatum.quality.common.rulebase;

import java.util.Arrays;

public enum RuleTypeNum {
    /** 类型定义 */
    CLASS("class"),
    REGEX("regex"),
    GLUE_GROOVY("glue_java"),
    COLUMN("column"),
    CODE("code");
    public final String value;

    RuleTypeNum(String value) {
        this.value = value;
    }

    public static RuleTypeNum getEnumByValue(String value) {
        if (value == null) {
            return null;
        }
        return Arrays.stream(RuleTypeNum.values())
                .filter(ruleTypeNum -> ruleTypeNum.value.equals(value))
                .findFirst()
                .orElse(null);
    }
}
