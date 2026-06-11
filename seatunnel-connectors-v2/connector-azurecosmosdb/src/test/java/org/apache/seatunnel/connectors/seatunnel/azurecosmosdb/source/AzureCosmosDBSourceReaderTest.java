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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureCosmosDBSourceReaderTest {

    @Test
    public void testUsesConfiguredQueryPageSize() {
        AzureCosmosDBSourceReader reader =
                new AzureCosmosDBSourceReader(null, createConfig(37), createRowType());

        Assertions.assertEquals(37, reader.getQueryPageSize());
    }

    private AzureCosmosDBConfig createConfig(int maxItemCount) {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());

        Map<String, Object> options = new HashMap<>();
        options.put("endpoint", "https://account.documents.azure.com:443/");
        options.put("primary_key", "account-key");
        options.put("database", "sales");
        options.put("container", "orders");
        options.put("query", "SELECT * FROM c");
        options.put("max_item_count", maxItemCount);
        options.put("schema", schema);
        return new AzureCosmosDBConfig(ReadonlyConfig.fromMap(options));
    }

    private SeaTunnelRowType createRowType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }
}
