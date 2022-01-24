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

package org.apache.seatunnel.config;

import org.apache.seatunnel.config.utils.FileUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import org.junit.Assert;
import org.junit.Test;

public class CompleteTest {

    @Test
    public void testVariables() {

        System.setProperty("dt", "20190318");
        System.setProperty("city2", "shanghai");

        Config config = ConfigFactory
            .parseFile(FileUtils.getFileFromResources("seatunnel/variables.conf"))
            .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        String sql1 = config.getConfigList("transform").get(1).getString("sql");
        String sql2 = config.getConfigList("transform").get(2).getString("sql");

        Assert.assertTrue(sql1.contains("shanghai"));
        Assert.assertTrue(sql2.contains("20190318"));

    }

}
