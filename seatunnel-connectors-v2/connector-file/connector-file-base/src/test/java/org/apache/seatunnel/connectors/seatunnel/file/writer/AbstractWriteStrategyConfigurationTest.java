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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ParquetWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/**
 * getConfiguration is called once per output file, so it caches the parsed resources. These tests
 * pin the part callers depend on: every call still yields a Configuration they may freely mutate.
 */
public class AbstractWriteStrategyConfigurationTest {

    private static ParquetWriteStrategy strategy(LocalFileSystemConf.LocalConf hadoopConf) {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", "file:///tmp/seatunnel/conf-cache/tmp");
        writeConfig.put("path", "file:///tmp/seatunnel/conf-cache");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        ParquetWriteStrategy strategy =
                new ParquetWriteStrategy(
                        new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), rowType));
        strategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
        strategy.init(hadoopConf, "job1", "job1", 0);
        return strategy;
    }

    @Test
    public void testEachCallReturnsAnIndependentlyMutableConfiguration() {
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        ParquetWriteStrategy strategy = strategy(hadoopConf);

        Configuration first = strategy.getConfiguration(hadoopConf);
        Configuration second = strategy.getConfiguration(hadoopConf);

        Assertions.assertNotSame(first, second);

        // Callers do mutate what they are handed - ParquetWriteStrategy#init sets
        // AvroWriteSupport.WRITE_FIXED_AS_INT96 on it - so a mutation must not be visible
        // to any later caller.
        first.set("seatunnel.test.marker", "written-by-first-caller");
        Assertions.assertNull(strategy.getConfiguration(hadoopConf).get("seatunnel.test.marker"));
        Assertions.assertNull(second.get("seatunnel.test.marker"));
    }

    @Test
    public void testConfigurationCarriesHadoopConfValues() {
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        ParquetWriteStrategy strategy = strategy(hadoopConf);

        // Same assertions on the first and a later call: the cached copy must not lose anything
        // that toConfiguration()/setExtraOptionsForConfiguration() put there.
        for (int call = 0; call < 3; call++) {
            Configuration configuration = strategy.getConfiguration(hadoopConf);
            Assertions.assertEquals(
                    FS_DEFAULT_NAME_DEFAULT, configuration.get("fs.defaultFS"), "call " + call);
            Assertions.assertEquals(
                    "org.apache.hadoop.fs.LocalFileSystem",
                    configuration.get("fs.file.impl"),
                    "call " + call);
            Assertions.assertTrue(
                    configuration.getBoolean("fs.file.impl.disable.cache", false), "call " + call);
            // A core-default.xml value, i.e. proof the copy carries the parsed resources and not
            // just the six properties toConfiguration() sets explicitly.
            Assertions.assertNotNull(configuration.get("io.file.buffer.size"), "call " + call);
        }
    }

    @Test
    public void testForeignHadoopConfIsNotServedFromTheCache() {
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        ParquetWriteStrategy strategy = strategy(hadoopConf);
        // Prime the cache.
        strategy.getConfiguration(hadoopConf);

        LocalFileSystemConf.LocalConf other =
                new LocalFileSystemConf.LocalConf("file:///tmp/seatunnel/other");
        Assertions.assertEquals(
                "file:///tmp/seatunnel/other",
                strategy.getConfiguration(other).get("fs.defaultFS"));
    }
}
