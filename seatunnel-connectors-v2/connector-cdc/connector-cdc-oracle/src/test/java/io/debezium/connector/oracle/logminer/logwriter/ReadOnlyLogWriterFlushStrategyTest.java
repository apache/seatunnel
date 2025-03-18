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

package io.debezium.connector.oracle.logminer.logwriter;

import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReadOnlyLogWriterFlushStrategyTest {

    @Test
    void returnsReadOnlyLogWriterFlushStrategyWhenReadOnlyKeyIsTrue() throws Exception {
        OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        Configuration configuration = mock(Configuration.class);
        when(config.getConfig()).thenReturn(configuration);
        when(configuration.getBoolean(OracleSourceConfigFactory.LOG_MINING_READONLY_KEY, false))
                .thenReturn(true);

        LogMinerStreamingChangeEventSource source =
                new LogMinerStreamingChangeEventSource(
                        config, null, null, null, null, null, null, null);
        LogWriterFlushStrategy strategy = source.resolveFlushStrategy();
        assertTrue(strategy instanceof ReadOnlyLogWriterFlushStrategy);

        Assertions.assertThrows(DebeziumException.class, () -> strategy.getHost());
        strategy.flush(null);
        strategy.close();
    }
}
