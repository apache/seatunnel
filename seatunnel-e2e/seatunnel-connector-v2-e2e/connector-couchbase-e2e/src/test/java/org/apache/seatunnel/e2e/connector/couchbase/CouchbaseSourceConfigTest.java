/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.couchbase;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.couchbase.source.CouchbaseSourceFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CouchbaseSourceConfigTest {
    @Test
    void bothSinksReferenceTheSourceOutput() {
        Config config = ConfigFactory.parseResources("couchbase_source_to_assert.conf");
        String sourceOutput = config.getConfigList("source").get(0).getString("plugin_output");
        assertEquals("couchbase_rows", sourceOutput);
        assertEquals(2, config.getConfigList("sink").size());
        config.getConfigList("sink")
                .forEach(sink -> assertEquals(sourceOutput, sink.getString("plugin_input")));
    }

    @Test
    void jobFixtureProvidesTheRequiredSchema() {
        ReadonlyConfig options =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseResources("couchbase_source_to_assert.conf")
                                .getConfigList("source")
                                .get(0));
        assertArrayEquals(
                new String[] {"id", "name", "enabled", "amount", "nullable"},
                new CouchbaseSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(options, getClass().getClassLoader()))
                        .createSource()
                        .getProducedCatalogTables()
                        .get(0)
                        .getSeaTunnelRowType()
                        .getFieldNames());
    }
}
