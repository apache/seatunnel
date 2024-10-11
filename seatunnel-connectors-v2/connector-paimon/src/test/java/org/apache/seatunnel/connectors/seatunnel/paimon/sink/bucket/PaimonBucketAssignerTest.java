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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonBucketAssignerTest {

    private Table table;
    private static final String TABLE_NAME = "default_table";
    private static final String DATABASE_NAME = "default_database";

    @BeforeEach
    public void before() throws Exception {
        boolean isWindows =
                System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");
        Options options = new Options();
        if (isWindows) {
            options.set("warehouse", "C:/Users/" + System.getProperty("user.name") + "/tmp/paimon");
        } else {
            options.set("warehouse", "file:///tmp/paimon");
        }
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE_NAME, true);
        Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
        if (!catalog.tableExists(identifier)) {
            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.column("id", DataTypes.INT(), "primary Key");
            schemaBuilder.column("name", DataTypes.STRING(), "name");
            schemaBuilder.primaryKey("id");
            schemaBuilder.option("bucket", "-1");
            schemaBuilder.option("dynamic-bucket.target-row-num", "20");
            Schema schema = schemaBuilder.build();
            catalog.createTable(identifier, schema, false);
        }
        table = catalog.getTable(identifier);
    }

    @Test
    public void bucketAssigner() {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        RowPartitionKeyExtractor keyExtractor =
                new RowPartitionKeyExtractor(fileStoreTable.schema());
        PaimonBucketAssigner paimonBucketAssigner = new PaimonBucketAssigner(fileStoreTable, 1, 0);
        Map<Integer, Integer> bucketInformation = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            GenericRow row = GenericRow.of(i, BinaryString.fromString(String.valueOf(i)));
            int assign = paimonBucketAssigner.assign(row);
            int hashCode = keyExtractor.trimmedPrimaryKey(row).hashCode();
            bucketInformation.put(hashCode, assign);
        }
        List<Integer> bucketSize =
                bucketInformation.values().stream().distinct().collect(Collectors.toList());
        Assertions.assertEquals(3, bucketSize.size());
    }
}
