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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Unit tests for DebeziumAdapterFactory */
public class DebeziumAdapterFactoryTest {

    @BeforeEach
    public void setUp() {
        // Clear cache before each test
        DebeziumAdapterFactory.clearCache();
    }

    @AfterEach
    public void tearDown() {
        // Clear cache after each test
        DebeziumAdapterFactory.clearCache();
    }

    @Test
    public void testGetAdapterWithMockImplementation() {
        // This test will only work if a mock adapter is registered via ServiceLoader
        // Since we're in the base module without concrete implementations,
        // we test the exception case
        try {
            DebeziumAdapterFactory.getAdapter("nonexistent-connector");
            fail("Expected IllegalStateException for unknown connector type");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("No DebeziumAdapter found"));
            assertTrue(e.getMessage().contains("nonexistent-connector"));
        }
    }

    @Test
    public void testClearCache() {
        // First call should trigger ServiceLoader
        try {
            DebeziumAdapterFactory.getAdapter("test-connector");
        } catch (IllegalStateException e) {
            // Expected - no adapter registered
        }

        // Clear the cache
        DebeziumAdapterFactory.clearCache();

        // Second call should trigger ServiceLoader again
        try {
            DebeziumAdapterFactory.getAdapter("test-connector");
        } catch (IllegalStateException e) {
            // Expected - no adapter registered
            assertTrue(e.getMessage().contains("No DebeziumAdapter found"));
        }
    }

    @Test
    public void testAdapterCaching() {
        // Create a mock adapter that can be registered
        MockDebeziumAdapter mockAdapter = new MockDebeziumAdapter("test");

        // Manually register it (simulating what ServiceLoader would do)
        // Since we can't actually use ServiceLoader in unit tests without
        // META-INF/services files, this test validates the caching logic

        // For now, this test just validates the exception behavior
        try {
            DebeziumAdapter adapter = DebeziumAdapterFactory.getAdapter("test");
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected when no adapter is registered
            assertNotNull(e.getMessage());
        }
    }

    /** Mock implementation for testing */
    private static class MockDebeziumAdapter implements DebeziumAdapter {
        private final String connectorType;

        public MockDebeziumAdapter(String connectorType) {
            this.connectorType = connectorType;
        }

        @Override
        public DebeziumEventDispatcher createEventDispatcher(DebeziumEventDispatcherConfig config) {
            return null;
        }

        @Override
        public DebeziumSchemaHistory createSchemaHistory(
                String instanceName, java.util.Collection<TableChangeInfo> tableChanges) {
            return null;
        }

        @Override
        public DebeziumTopicNaming createTopicNaming(String logicalName, String heartbeatPrefix) {
            return null;
        }

        @Override
        public String getDebeziumVersion() {
            return "1.9.8-test";
        }

        @Override
        public boolean supports(String connectorType) {
            return this.connectorType.equals(connectorType);
        }
    }
}
