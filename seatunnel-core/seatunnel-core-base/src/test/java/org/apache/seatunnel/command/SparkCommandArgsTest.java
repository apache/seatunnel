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

package org.apache.seatunnel.command;

import org.apache.seatunnel.common.config.DeployMode;

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SparkCommandArgsTest {

    @Test
    public void testParseSparkArgs() {
        String[] args = {"-c", "app.conf", "-e", "client", "-m", "yarn", "-i", "city=shijiazhuang", "-i", "name=Tom"};
        SparkCommandArgs sparkArgs = new SparkCommandArgs();
        JCommander.newBuilder()
            .addObject(sparkArgs)
            .build()
            .parse(args);
        Assert.assertEquals("app.conf", sparkArgs.getConfigFile());
        Assert.assertEquals(DeployMode.CLIENT, sparkArgs.getDeployMode());
        Assert.assertEquals("yarn", sparkArgs.getMaster());
        Assert.assertEquals(Arrays.asList("city=shijiazhuang", "name=Tom"), sparkArgs.getVariables());
    }
}
