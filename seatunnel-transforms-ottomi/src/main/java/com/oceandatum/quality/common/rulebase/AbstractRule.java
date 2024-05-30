package com.oceandatum.quality.common.rulebase;

import java.util.regex.Pattern;

public abstract class AbstractRule implements IRuleHandler {

    public String rule;

    public AbstractRule() {}

    public AbstractRule(String rule) {
        this.rule = rule;
    }

    /**
     * 默认实现方式正则表达式
     *
     * @param value
     * @return
     */
    @Override
    public boolean doCheck(String value) {
        return Pattern.matches(rule, value);
    }

    @Override
    public boolean doCheck(String value, String target) {
        return false;
    }
}
