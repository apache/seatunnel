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

package org.apache.seatunnel.flink.assertion;

import org.apache.seatunnel.api.configuration.util.ConditionOperator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AssertFactoryTest {

    @Test
    public void testOptionRule() throws Exception {
        AssertSinkFactory factory = new AssertSinkFactory();
        OptionRule optionRule = factory.optionRule();
        Assertions.assertNotNull(optionRule);
        Assertions.assertEquals(1, optionRule.getValueConstraints().size());
        Assertions.assertEquals(
                ConditionOperator.MAP_NOT_EMPTY,
                optionRule.getValueConstraints().get(0).getOperator());
        Assertions.assertEquals(
                AssertSinkOptions.RULES, optionRule.getValueConstraints().get(0).getOption());
    }
}
