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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;

public class ServerExecuteCommandTest {

    @Test
    @DisabledOnJre(value = JRE.JAVA_11, disabledReason = "the test case only works on Java 8")
    public void testJavaVersionCheck() {
        String realVersion = System.getProperty("java.version");
        System.setProperty("java.version", "1.8.0_191");
        Assertions.assertFalse(ServerExecuteCommand.isAllocatingThreadGetName());
        System.setProperty("java.version", "1.8.0_60");
        Assertions.assertTrue(ServerExecuteCommand.isAllocatingThreadGetName());
        System.setProperty("java.version", realVersion);
    }
}
