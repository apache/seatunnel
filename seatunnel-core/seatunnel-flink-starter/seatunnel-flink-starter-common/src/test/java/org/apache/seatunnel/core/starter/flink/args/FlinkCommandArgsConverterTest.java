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

package org.apache.seatunnel.core.starter.flink.args;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.enums.MasterType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;

/**
 * Verifies that the Flink argument converters resolve {@code --deploy-mode} and {@code --target}
 * values independently of the JVM default locale.
 */
@ResourceLock("java.util.Locale")
public class FlinkCommandArgsConverterTest {

    @Test
    public void testDeployModeConverter() {
        FlinkCommandArgs.FlinkDeployModeConverter converter =
                new FlinkCommandArgs.FlinkDeployModeConverter();

        Assertions.assertEquals(DeployMode.RUN, converter.convert("run"));
        Assertions.assertEquals(DeployMode.RUN, converter.convert("RUN"));
        Assertions.assertEquals(DeployMode.RUN_APPLICATION, converter.convert("run-application"));
        Assertions.assertEquals(DeployMode.RUN_APPLICATION, converter.convert("RUN-APPLICATION"));
    }

    @Test
    public void testMasterTargetConverter() {
        FlinkCommandArgs.FlinkMasterTargetConverter converter =
                new FlinkCommandArgs.FlinkMasterTargetConverter();

        Assertions.assertEquals(MasterType.LOCAL, converter.convert("local"));
        Assertions.assertEquals(MasterType.YARN_SESSION, converter.convert("yarn-session"));
        Assertions.assertEquals(
                MasterType.KUBERNETES_APPLICATION, converter.convert("kubernetes-application"));
    }

    @Test
    public void testConvertersAreLocaleIndependent() {
        Locale base = Locale.getDefault();
        Locale display = Locale.getDefault(Locale.Category.DISPLAY);
        Locale format = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            FlinkCommandArgs.FlinkDeployModeConverter deployModeConverter =
                    new FlinkCommandArgs.FlinkDeployModeConverter();
            Assertions.assertEquals(
                    DeployMode.RUN_APPLICATION, deployModeConverter.convert("run-application"));

            FlinkCommandArgs.FlinkMasterTargetConverter masterTargetConverter =
                    new FlinkCommandArgs.FlinkMasterTargetConverter();
            Assertions.assertEquals(
                    MasterType.YARN_SESSION, masterTargetConverter.convert("yarn-session"));
            Assertions.assertEquals(
                    MasterType.KUBERNETES_APPLICATION,
                    masterTargetConverter.convert("kubernetes-application"));
        } finally {
            Locale.setDefault(base);
            Locale.setDefault(Locale.Category.DISPLAY, display);
            Locale.setDefault(Locale.Category.FORMAT, format);
        }
    }
}
