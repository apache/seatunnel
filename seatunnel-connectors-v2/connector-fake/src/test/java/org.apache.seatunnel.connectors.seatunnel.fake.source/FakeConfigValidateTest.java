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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.configuration.CheckResult;
import org.apache.seatunnel.api.configuration.util.OptionUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FakeConfigValidateTest {
    @Test
    public void testConfigValidate() {
        Config config = ConfigFactory.empty();
        CheckResult checkResult = OptionUtil.configCheck(config, new FakeSourceFactory().optionRule());
        Assertions.assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - " +
                        "There are unconfigured options, the options('schema') are required.",
                checkResult.getMsg());
        Assertions.assertFalse(checkResult.isSuccess());
    }
}
