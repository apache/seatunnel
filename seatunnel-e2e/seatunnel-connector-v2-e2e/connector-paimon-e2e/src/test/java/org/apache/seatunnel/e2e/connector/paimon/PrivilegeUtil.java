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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;

import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class PrivilegeUtil {
    private PrivilegeUtil() {}

    public static void awaitSelectAndInsertPrivilegeApplied(TestContainer container) {
        awaitPrivilegeApplied(container, "paimon_to_paimon_privilege.conf", true);
    }

    public static void awaitInsertPrivilegeApplied(TestContainer container) {
        awaitPrivilegeApplied(container, "fake_to_paimon_privilege.conf", true);
    }

    public static void awaitPrivilegeApplied(
            TestContainer container, String confPath, boolean expectSuccess) {
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult result = container.executeJob(confPath);
                            if (expectSuccess) {
                                Assertions.assertEquals(
                                        0,
                                        result.getExitCode(),
                                        "Expected job success but failed: " + result.getStderr());
                            } else {
                                Assertions.assertNotEquals(
                                        0,
                                        result.getExitCode(),
                                        "Expected job failure but succeeded");
                            }
                        });
    }
}
