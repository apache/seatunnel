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

package org.apache.seatunnel.core.base.utils;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.base.command.AbstractCommandArgs;

import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtilsTest {

    @Test
    public void getConfigPath() throws URISyntaxException {
        // test client mode.
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        Path expectConfPath = Paths.get(FileUtilsTest.class.getResource("/flink.batch.conf").toURI());
        sparkCommandArgs.setConfigFile(expectConfPath.toString());
        Assert.assertEquals(expectConfPath, FileUtils.getConfigPath(sparkCommandArgs));

        // test cluster mode
        sparkCommandArgs.setDeployMode(DeployMode.CLUSTER);
        Assert.assertEquals("flink.batch.conf", FileUtils.getConfigPath(sparkCommandArgs).toString());
    }

    private static class SparkCommandArgs extends AbstractCommandArgs {
        private DeployMode deployMode;

        public void setDeployMode(DeployMode deployMode) {
            this.deployMode = deployMode;
        }

        public DeployMode getDeployMode() {
            return deployMode;
        }
    }
}
