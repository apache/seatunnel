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
package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.debezium.jdbc.JdbcConnection;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OracleConnectionUtilsTest {

    private static final String SHOW_CON_NAME =
            "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') CON_NAME FROM DUAL";

    @Mock private JdbcConnection jdbcConnection;

    @Test
    public void testGetCurrentContainerNameSuccess() throws SQLException {
        // Prepare test data
        String expectedContainerName = "CDB$ROOT";

        // Mock database query result
        when(jdbcConnection.queryAndMap(eq(SHOW_CON_NAME), any()))
                .thenReturn(expectedContainerName);

        // Execute test
        String actualContainerName = OracleConnectionUtils.getCurrentContainerName(jdbcConnection);

        // Verify result
        assertEquals(expectedContainerName, actualContainerName);
    }
}
