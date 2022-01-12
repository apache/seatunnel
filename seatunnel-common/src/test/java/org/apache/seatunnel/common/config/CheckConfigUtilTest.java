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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CheckConfigUtilTest {

    @Test
    public void testCheck() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("k0", "v0");
        configMap.put("k1", "v1");
        Config config = ConfigFactory.parseMap(configMap);

        CheckResult check = CheckConfigUtil.check(config, "k0", "k1");
        Assert.assertTrue(check.isSuccess());

        String errorMsg = "please specify [%s] as non-empty";
        check = CheckConfigUtil.check(config, "k0", "k1", "k2");
        Assert.assertEquals(String.format(errorMsg, "k2"), check.getMsg());

        check = CheckConfigUtil.check(config, "k0", "k1", "k2", "k3", "k4");
        Assert.assertEquals(String.format(errorMsg, "k2,k3,k4"), check.getMsg());
    }
}
