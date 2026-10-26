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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.MapConfig;

/**
 * Verifies every dirty-job map is configured with synchronous backups before cluster startup.
 *
 * <p>This prevents a partition-owner change from silently discarding tracking evidence.
 */
class DirtyJobMapConfigTest {

    @Test
    void shouldConfigureSynchronousBackupsBeforeMapCreation() {
        SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().setBackupCount(2);

        SeaTunnelServerStarter.configureDirtyJobMaps(seaTunnelConfig);

        assertMapConfig(seaTunnelConfig, Constant.IMAP_DIRTY_JOB_STATE);
        assertMapConfig(seaTunnelConfig, Constant.IMAP_DIRTY_JOB_MEMBER_EVENT_SEQUENCE);
        assertMapConfig(seaTunnelConfig, Constant.IMAP_DIRTY_JOB_PENDING_MEMBER_EVENTS);
        assertMapConfig(seaTunnelConfig, Constant.IMAP_DIRTY_JOB_ENABLED_THRESHOLDS);
    }

    private void assertMapConfig(SeaTunnelConfig seaTunnelConfig, String mapName) {
        MapConfig mapConfig = seaTunnelConfig.getHazelcastConfig().getMapConfigs().get(mapName);
        Assertions.assertNotNull(mapConfig);
        Assertions.assertEquals(2, mapConfig.getBackupCount());
        Assertions.assertEquals(0, mapConfig.getAsyncBackupCount());
    }
}
