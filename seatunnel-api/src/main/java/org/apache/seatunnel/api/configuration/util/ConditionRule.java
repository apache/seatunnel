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

package org.apache.seatunnel.api.configuration.util;

import java.util.Objects;

public class ConditionRule {

    private final Condition<?> condition;
    private final OptionRule optionRule;

    public ConditionRule(Condition<?> condition, OptionRule optionRule) {
        this.condition = condition;
        this.optionRule = optionRule;
    }

    public Condition<?> getCondition() {
        return condition;
    }

    public OptionRule getOptionRule() {
        return optionRule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConditionRule)) {
            return false;
        }
        ConditionRule that = (ConditionRule) o;
        return Objects.equals(condition, that.condition)
                && Objects.equals(optionRule, that.optionRule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, optionRule);
    }
}
