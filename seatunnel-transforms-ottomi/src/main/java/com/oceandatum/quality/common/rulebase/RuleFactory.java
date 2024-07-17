/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
