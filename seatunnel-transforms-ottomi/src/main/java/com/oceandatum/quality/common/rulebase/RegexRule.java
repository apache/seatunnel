package com.oceandatum.quality.common.rulebase;

import java.util.regex.Pattern;

/** 正则表达式 */
public class RegexRule extends AbstractRule {

    public RegexRule(String rule) {
        super(rule);
    }

    @Override
    public boolean doCheck(String value) {
        if (null == value) {
            return false;
        }
        return Pattern.matches(rule, value);
    }

    @Override
    public boolean doCheck(String value, String target) {
        return false;
    }
}
