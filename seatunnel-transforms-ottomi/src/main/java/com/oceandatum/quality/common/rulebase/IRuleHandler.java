package com.oceandatum.quality.common.rulebase;

/** 规则检验抽象类 */
public interface IRuleHandler {

    public boolean doCheck(String value);

    public boolean doCheck(String value, String target);
}
