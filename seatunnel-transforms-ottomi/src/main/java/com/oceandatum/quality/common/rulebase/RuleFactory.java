package com.oceandatum.quality.common.rulebase;

/**
 * 规则获取工产类
 *
 * @author zhangzhifeng
 */
public class RuleFactory {
    public static IRuleHandler getRule(String type, String value) {
        // 自定义方法校验
        if (RuleTypeNum.CLASS.value.equals(type)) {
            try {
                // 利用反射进行类实例化
                Class cls = Class.forName(value);
                Object obj = cls.newInstance();
                if (obj instanceof IRuleHandler) {
                    return (IRuleHandler) obj;
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        } else if (RuleTypeNum.REGEX.value.equals(type)) {
            return new RegexRule(value);
        } else if (RuleTypeNum.GLUE_GROOVY.value.equals(type)) {
            try {
                return GlueFactory.getInstance().loadNewInstance(value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
