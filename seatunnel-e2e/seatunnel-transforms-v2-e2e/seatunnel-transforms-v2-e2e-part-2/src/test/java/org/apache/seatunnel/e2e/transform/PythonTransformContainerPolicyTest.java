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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.spark.AbstractTestSparkContainer;
import org.apache.seatunnel.e2e.common.container.spark.Spark2Container;
import org.apache.seatunnel.e2e.common.container.spark.Spark35Container;
import org.apache.seatunnel.e2e.common.container.spark.Spark3Container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Method;

class PythonTransformContainerPolicyTest {

    @ParameterizedTest
    @EnumSource(
            value = TestContainerId.class,
            names = {"SPARK_2_4", "SPARK_3_3", "SPARK_3_5"})
    void preservesSparkRuntimeAndCommandWithPolicyInJobConfig(TestContainerId containerId)
            throws Exception {
        TestContainer original;
        switch (containerId) {
            case SPARK_2_4:
                original = new Spark2Container();
                break;
            case SPARK_3_3:
                original = new Spark3Container();
                break;
            case SPARK_3_5:
                original = new Spark35Container();
                break;
            default:
                throw new IllegalArgumentException("Unsupported Spark container " + containerId);
        }
        TestContainer prepared = TestPythonTransformIT.withPythonTransformPolicy(original);
        Assertions.assertEquals(containerId, prepared.identifier());
        Assertions.assertEquals(original.getClass(), prepared.getClass().getSuperclass());
        Method command = AbstractTestSparkContainer.class.getDeclaredMethod("getDockerCommand");
        command.setAccessible(true);
        Assertions.assertEquals(command.invoke(original), command.invoke(prepared));
        Method shell = AbstractTestContainer.class.getDeclaredMethod("getStartShellName");
        shell.setAccessible(true);
        Assertions.assertEquals(shell.invoke(original), shell.invoke(prepared));
        for (Method override : prepared.getClass().getDeclaredMethods()) {
            Assertions.assertEquals("getDockerImage", override.getName());
        }
    }
}
