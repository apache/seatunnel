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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

public class FileSinkConfigTest {

    @Test
    public void testConfigInit() throws Exception {
        URL conf = OrcReadStrategyTest.class.getResource("/test_write_hdfs.conf");
        Assertions.assertNotNull(conf);
        String confPath = Paths.get(conf.toURI()).toString();
        Config config = ConfigFactory.parseFile(new File(confPath));

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"data", "ts"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        Assertions.assertDoesNotThrow(() -> new FileSinkConfig(config, rowType));
    }

    @Test
    public void testSinkColumnsGreaterThanSource() throws Exception {
        URL conf = OrcReadStrategyTest.class.getResource("/test_write_hive.conf");
        Assertions.assertNotNull(conf);
        String confPath = Paths.get(conf.toURI()).toString();
        Config config = ConfigFactory.parseFile(new File(confPath));

        SeaTunnelRowType seaTunnelRowTypeInfo =
                new SeaTunnelRowType(
                        new String[] {"name", "age", "address"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        FileSinkConfig fileSinkConfig = new FileSinkConfig(config, seaTunnelRowTypeInfo);
        List<Integer> sinkColumnsIndexInRow = fileSinkConfig.getSinkColumnsIndexInRow();
        Assertions.assertEquals(
                sinkColumnsIndexInRow.size(), seaTunnelRowTypeInfo.getFieldNames().length);
    }
}
