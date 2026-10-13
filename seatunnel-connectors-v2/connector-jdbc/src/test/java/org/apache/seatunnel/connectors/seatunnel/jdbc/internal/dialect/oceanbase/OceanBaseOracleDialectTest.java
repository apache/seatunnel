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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;

import org.junit.jupiter.api.Test;

import com.oceanbase.jdbc.UrlParser;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OceanBaseOracleDialectTest {

    @Test
    public void testOnlyOceanBaseOracleDisablesSamplingSharding() {
        assertFalse(new OceanBaseOracleDialect().supportsSamplingSharding());
        assertTrue(new OceanBaseMysqlDialect().supportsSamplingSharding());
        assertTrue(new OracleDialect().supportsSamplingSharding());
    }

    @Test
    public void testSourceConnectionForcesServerSideCursor() throws Exception {
        String url =
                "jdbc:oceanbase://localhost:2881/test?useServerPrepStmts=false&useCursorFetch=false";
        Map<String, String> info = new HashMap<>();
        info.put("useServerPrepStmts", "false");

        new OceanBaseOracleDialect().configureSourceConnection(url, info);

        assertEquals("true", info.get("useServerPrepStmts"));
        Properties properties = new Properties();
        properties.putAll(info);
        assertTrue(UrlParser.parse(url, properties).getOptions().useServerPrepStmts);
    }

    @Test
    public void testSinkConnectionKeepsUserConfiguration() {
        String url = "jdbc:oceanbase://localhost:2881/test";
        Map<String, String> sinkInfo = new HashMap<>();
        OceanBaseOracleDialect dialect = new OceanBaseOracleDialect();

        dialect.connectionUrlParse(url, sinkInfo, dialect.defaultParameter());

        assertFalse(sinkInfo.containsKey("useServerPrepStmts"));
    }

    @Test
    public void testFactoryCreatesOceanBaseOracleDialectForOracleMode() {
        OceanBaseDialectFactory factory = new OceanBaseDialectFactory();

        JdbcDialect dialect = factory.create("oracle", "`");

        assertTrue(dialect instanceof OceanBaseOracleDialect);
        // The dialect name must stay Oracle: Oracle-specific execution paths are keyed on it.
        assertEquals(DatabaseIdentifier.ORACLE, dialect.dialectName());
        assertFalse(dialect.supportsSamplingSharding());
    }
}
