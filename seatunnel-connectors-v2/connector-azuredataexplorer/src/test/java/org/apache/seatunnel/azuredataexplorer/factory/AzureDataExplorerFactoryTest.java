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

package org.apache.seatunnel.azuredataexplorer.factory;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.api.options.SourceConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions;
import org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions;
import org.apache.seatunnel.azuredataexplorer.sink.AzureDataExplorerSink;
import org.apache.seatunnel.azuredataexplorer.sink.AzureDataExplorerSinkFactory;
import org.apache.seatunnel.azuredataexplorer.source.AzureDataExplorerSource;
import org.apache.seatunnel.azuredataexplorer.source.AzureDataExplorerSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureDataExplorerFactoryTest {

    @Test
    public void testSinkFactoryRuleAndCreation() {
        AzureDataExplorerSinkFactory factory = new AzureDataExplorerSinkFactory();
        OptionRule rule = factory.optionRule();

        List<Option<?>> requiredOptions = flattenRequiredOptions(rule);
        Assertions.assertTrue(requiredOptions.contains(AzureDataExplorerSinkOptions.CLUSTER_URI));
        Assertions.assertTrue(requiredOptions.contains(AzureDataExplorerSinkOptions.DATABASE));
        Assertions.assertTrue(requiredOptions.contains(AzureDataExplorerSinkOptions.TABLE));

        Assertions.assertTrue(
                rule.getOptionalOptions()
                        .contains(AzureDataExplorerSinkOptions.INGESTION_MAPPING_REFERENCE));
        Assertions.assertTrue(
                rule.getOptionalOptions().contains(AzureDataExplorerSinkOptions.INGESTION_TYPE));
        Assertions.assertTrue(
                rule.getOptionalOptions().contains(AzureDataExplorerSinkOptions.BATCH_SIZE));
        Assertions.assertTrue(
                rule.getOptionalOptions().contains(AzureDataExplorerSinkOptions.FLUSH_INTERVAL_MS));

        Assertions.assertTrue(hasBundledCredentialOptions(rule));

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("cluster_uri", "https://example.kusto.windows.net");
        configMap.put("database", "db");
        configMap.put("table", "table_a");
        configMap.put("client_id", "client");
        configMap.put("client_secret", "secret");
        configMap.put("tenant_id", "tenant");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        null, config, Thread.currentThread().getContextClassLoader());

        TableSink tableSink = factory.createSink(context);
        SeaTunnelSink sink = tableSink.createSink();
        Assertions.assertInstanceOf(AzureDataExplorerSink.class, sink);
    }

    @Test
    public void testSourceFactoryRuleAndCreation() {
        AzureDataExplorerSourceFactory factory = new AzureDataExplorerSourceFactory();
        OptionRule rule = factory.optionRule();

        List<Option<?>> requiredOptions = flattenRequiredOptions(rule);
        Assertions.assertTrue(requiredOptions.contains(AzureDataExplorerSourceOptions.CLUSTER_URI));
        Assertions.assertTrue(requiredOptions.contains(AzureDataExplorerSourceOptions.DATABASE));
        Assertions.assertTrue(requiredOptions.contains(AzureDataExplorerSourceOptions.QUERY));
        Assertions.assertTrue(
                rule.getOptionalOptions().contains(SourceConnectorCommonOptions.SCHEMA));

        Assertions.assertTrue(hasBundledCredentialOptions(rule));

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("cluster_uri", "https://example.kusto.windows.net");
        configMap.put("database", "db");
        configMap.put("query", "MyTable | take 10");
        configMap.put("client_id", "client");
        configMap.put("client_secret", "secret");
        configMap.put("tenant_id", "tenant");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        config, Thread.currentThread().getContextClassLoader());

        TableSource<?, ?, ?> tableSource = factory.createSource(context);
        SeaTunnelSource<?, ?, ?> source = tableSource.createSource();
        Assertions.assertInstanceOf(AzureDataExplorerSource.class, source);
    }

    private static List<Option<?>> flattenRequiredOptions(OptionRule rule) {
        List<Option<?>> options = new ArrayList<>();
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            options.addAll(requiredOption.getOptions());
        }
        return options;
    }

    private static boolean hasBundledCredentialOptions(OptionRule rule) {
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            if (requiredOption instanceof RequiredOption.BundledRequiredOptions) {
                List<Option<?>> options = requiredOption.getOptions();
                if (options.contains(AzureDataExplorerSinkOptions.CLIENT_ID)
                        && options.contains(AzureDataExplorerSinkOptions.CLIENT_SECRET)
                        && options.contains(AzureDataExplorerSinkOptions.TENANT_ID)) {
                    return true;
                }
                if (options.contains(AzureDataExplorerSourceOptions.CLIENT_ID)
                        && options.contains(AzureDataExplorerSourceOptions.CLIENT_SECRET)
                        && options.contains(AzureDataExplorerSourceOptions.TENANT_ID)) {
                    return true;
                }
            }
        }
        return false;
    }
}
