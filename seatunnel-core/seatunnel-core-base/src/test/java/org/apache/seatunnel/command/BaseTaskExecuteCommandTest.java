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

import junit.framework.TestCase;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BaseTaskExecuteCommandTest extends TestCase {

    private static int closeTimes = 0;

    @Test
    public void testClose() {
        List<MockPlugin> pluginListA = new ArrayList<>();
        pluginListA.add(new MockPlugin());
        pluginListA.add(new MockPlugin());
        List<MockPlugin> pluginListB = new ArrayList<>();
        pluginListB.add(new MockPlugin());
        pluginListB.add(new MockPlugin());
        MockTaskExecutorCommand mockTaskExecutorCommand = new MockTaskExecutorCommand();
        try {
            mockTaskExecutorCommand.close(pluginListA, pluginListB);
        } catch (Exception ex) {
            // ignore
        }
        Assert.assertEquals(4, closeTimes);
        Assert.assertThrows(RuntimeException.class, () -> mockTaskExecutorCommand.close(pluginListA));

    }

    private static class MockPlugin implements Plugin<FlinkEnvironment> {

        @Override
        public void setConfig(Config config) {
        }

        @Override
        public Config getConfig() {
            return null;
        }

        @Override
        public void close() {
            closeTimes++;
            throw new RuntimeException("Test close with exception");
        }
    }

    private static class MockTaskExecutorCommand extends BaseTaskExecuteCommand<FlinkCommandArgs, FlinkEnvironment> {

        @Override
        public void execute(FlinkCommandArgs commandArgs) {

        }
    }
}