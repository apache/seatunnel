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

package org.apache.seatunnel.e2e.spark.fake;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * This test case is used to verify that the fake source is able to send data to the console.
 * Make sure the SeaTunnel job can submit successfully on spark engine.
 */
public class FakeSourceToConsoleIT extends SparkContainer {

    @Test
    public void testFakeSourceToConsoleSine() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/fake/fakesource_to_console.conf");
        Assert.assertEquals(0, execResult.getExitCode());
    }
}
