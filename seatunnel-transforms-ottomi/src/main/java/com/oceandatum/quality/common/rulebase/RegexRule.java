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
