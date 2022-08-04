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

package org.apache.seatunnel.common.config;

import static org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists;
import static org.apache.seatunnel.common.config.CheckConfigUtil.checkAtLeastOneExists;
import static org.apache.seatunnel.common.config.CheckConfigUtil.mergeCheckResults;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CheckConfigUtilTest {

    @Test
    public void testCheckAllExists() {
        Config config = getConfig();
        CheckResult checkResult = checkAllExists(config, "k0", "k1");
        Assertions.assertTrue(checkResult.isSuccess());

        String errorMsg = "please specify [%s] as non-empty";
        checkResult = checkAllExists(config, "k0", "k1", "k2");
        Assertions.assertEquals(String.format(errorMsg, "k2"), checkResult.getMsg());

        checkResult = checkAllExists(config, "k0", "k1", "k2", "k3", "k4");
        Assertions.assertEquals(String.format(errorMsg, "k2,k3,k4"), checkResult.getMsg());
    }

    @Test
    public void testCheckAtLeastOneExists() {
        Config config = getConfig();
        CheckResult checkResult = checkAtLeastOneExists(config, "k0", "k3", "k4");
        Assertions.assertTrue(checkResult.isSuccess());

        String errorMsg = "please specify at least one config of [%s] as non-empty";
        checkResult = checkAtLeastOneExists(config, "k3", "k2");
        Assertions.assertEquals(String.format(errorMsg, "k3,k2"), checkResult.getMsg());
    }

    @Test
    public void testMergeCheckResults() {
        Config config = getConfig();
        CheckResult checkResult1 = checkAllExists(config, "k0", "k1");
        CheckResult checkResult2 = checkAtLeastOneExists(config, "k1", "k3");
        CheckResult checkResult3 = checkAllExists(config, "k0", "k3");
        CheckResult checkResult4 = checkAtLeastOneExists(config, "k2", "k3");

        CheckResult finalResult = mergeCheckResults(checkResult1, checkResult2);
        Assertions.assertTrue(finalResult.isSuccess());

        String errorMsg1 = "please specify [%s] as non-empty";
        String errorMsg2 = "please specify at least one config of [%s] as non-empty";
        finalResult = mergeCheckResults(checkResult3, checkResult2);
        Assertions.assertEquals(String.format(errorMsg1, "k3"), finalResult.getMsg());

        finalResult = mergeCheckResults(checkResult3, checkResult4);
        Assertions.assertEquals(String.format(errorMsg1 + "," + errorMsg2, "k3", "k2,k3"), finalResult.getMsg());
    }

    public Config getConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("k0", "v0");
        configMap.put("k1", "v1");
        configMap.put("k2", new ArrayList<>());
        configMap.put("k3", null);
        return ConfigFactory.parseMap(configMap);
    }
}
