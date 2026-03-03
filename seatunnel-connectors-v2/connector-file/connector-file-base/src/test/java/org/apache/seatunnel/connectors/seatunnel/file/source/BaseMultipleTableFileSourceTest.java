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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class BaseMultipleTableFileSourceTest {

    @Test
    void testRejectPostSyncActionWhenDiscoveryModeOnce() {
        BaseMultipleTableFileSource source = createSource("once", "delete");
        FileConnectorException exception =
                Assertions.assertThrows(FileConnectorException.class, source::getBoundedness);
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("post_sync_action only supports discovery_mode=continuous"),
                "post_sync_action should be rejected when discovery_mode=once");
    }

    @Test
    void testAllowPostSyncActionNoneWhenDiscoveryModeOnce() {
        BaseMultipleTableFileSource source = createSource("once", "none");
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void testAllowPostSyncActionWhenDiscoveryModeContinuous() {
        BaseMultipleTableFileSource source = createSource("continuous", "delete");
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    private BaseMultipleTableFileSource createSource(String discoveryMode, String postSyncAction) {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.DISCOVERY_MODE.key(), discoveryMode);
        config.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), postSyncAction);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        BaseFileSourceConfig baseFileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(baseFileSourceConfig.getBaseFileSourceConfig()).thenReturn(readonlyConfig);

        BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleTableFileSourceConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(baseFileSourceConfig));

        return new TestMultipleTableFileSource(multipleTableFileSourceConfig);
    }

    private static final class TestMultipleTableFileSource extends BaseMultipleTableFileSource {

        private TestMultipleTableFileSource(
                BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig) {
            super(baseMultipleTableFileSourceConfig);
        }

        @Override
        public String getPluginName() {
            return "test_file_source";
        }
    }
}
