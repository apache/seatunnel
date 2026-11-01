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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Optional;

public class DeployModeTest {

    @Test
    public void testFromResolvesDeployMode() {
        Assertions.assertEquals(Optional.of(DeployMode.CLIENT), DeployMode.from("client"));
        Assertions.assertEquals(Optional.of(DeployMode.CLIENT), DeployMode.from("CLIENT"));
        Assertions.assertEquals(
                Optional.of(DeployMode.RUN_APPLICATION), DeployMode.from("run-application"));
        Assertions.assertEquals(Optional.empty(), DeployMode.from("unknown"));
        Assertions.assertEquals(Optional.empty(), DeployMode.from(null));
    }

    @Test
    public void testFromIsLocaleIndependent() {
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            Assertions.assertEquals(Optional.of(DeployMode.CLIENT), DeployMode.from("CLIENT"));
            Assertions.assertEquals(
                    Optional.of(DeployMode.RUN_APPLICATION), DeployMode.from("RUN-APPLICATION"));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
