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

package org.apache.seatunnel.core.starter.spark.args;

import org.apache.seatunnel.common.config.DeployMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class SparkCommandArgsConverterTest {

    @Test
    public void testDeployModeConverter() {
        SparkCommandArgs.SparkDeployModeConverter converter =
                new SparkCommandArgs.SparkDeployModeConverter();

        Assertions.assertEquals(DeployMode.CLIENT, converter.convert("client"));
        Assertions.assertEquals(DeployMode.CLUSTER, converter.convert("CLUSTER"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert("run"));
    }

    @Test
    public void testDeployModeConverterIsLocaleIndependent() {
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            SparkCommandArgs.SparkDeployModeConverter converter =
                    new SparkCommandArgs.SparkDeployModeConverter();
            Assertions.assertEquals(DeployMode.CLIENT, converter.convert("client"));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
