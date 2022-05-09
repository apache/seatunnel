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

package org.apache.seatunnel.core.base.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.seatunnel.apis.base.plugin.Plugin;
import org.apache.seatunnel.apis.base.plugin.PluginClosedException;
import org.apache.seatunnel.flink.FlinkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BaseTaskExecuteCommandTest {

    private static int CLOSE_TIMES = 0;

    @Before
    public void setUp() {
        CLOSE_TIMES = 0;
    }

    @Test
    public void testClose() {
        List<MockPlugin> pluginListA = new ArrayList<>();
        pluginListA.add(new MockPlugin());
        pluginListA.add(new MockPlugin());
        List<MockPlugin> pluginListB = new ArrayList<>();
        pluginListB.add(new MockPlugin());
        pluginListB.add(new MockPlugin());
        MockTaskExecutorCommand mockTaskExecutorCommand = new MockTaskExecutorCommand();
        mockTaskExecutorCommand.close(pluginListA, pluginListB);
        assertEquals(Integer.parseInt("0"), CLOSE_TIMES);
    }

    @Test
    public void testExceptionClose() {
        List<MockExceptionPlugin> pluginListA = new ArrayList<>();
        pluginListA.add(new MockExceptionPlugin());
        pluginListA.add(new MockExceptionPlugin());
        List<MockExceptionPlugin> pluginListB = new ArrayList<>();
        pluginListB.add(new MockExceptionPlugin());
        pluginListB.add(new MockExceptionPlugin());
        MockTaskExecutorCommand mockTaskExecutorCommand = new MockTaskExecutorCommand();
        try {
            mockTaskExecutorCommand.close(pluginListA, pluginListB);
        } catch (Exception ex) {
            // just print into console
            ex.printStackTrace();
        }
        assertEquals(Integer.parseInt("4"), CLOSE_TIMES);
        assertThrows(PluginClosedException.class, () -> mockTaskExecutorCommand.close(pluginListA));
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

        }

    }

    private static class MockExceptionPlugin implements Plugin<FlinkEnvironment> {

        @Override
        public void setConfig(Config config) {
        }

        @Override
        public Config getConfig() {
            return null;
        }

        @Override
        public void close() {
            CLOSE_TIMES++;
            throw new PluginClosedException("Test close with exception, closeTimes:" + CLOSE_TIMES);
        }

    }

    private static class MockTaskExecutorCommand extends BaseTaskExecuteCommand<AbstractCommandArgs, FlinkEnvironment> {

        @Override
        public void execute() {

        }
    }
}
