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

package org.apache.seatunnel.connectors.seatunnel.woocommerce.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.JobMode;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WooCommerceSourceTest {
    @Test
    void acceptsEngineInjectedFailFastWithoutAdvertisingMultiTableSupport() {
        ReadonlyConfig injected =
                MultiTableFailureHelper.withMultiTableFailurePolicy(
                        ReadonlyConfig.fromMap(WooCommerceClientTest.options()),
                        ReadonlyConfig.fromMap(Collections.emptyMap()));
        new WooCommerceSource(injected);
        Map<String, Object> values = WooCommerceClientTest.options();
        values.put(
                MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key(), "CONTINUE_OTHER_TABLES");
        assertThrows(
                IllegalArgumentException.class,
                () -> new WooCommerceSource(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void factoryCreatesRuntimeSourceAndRejectsStreaming() {
        WooCommerceSourceFactory factory = new WooCommerceSourceFactory();
        assertEquals(WooCommerceSource.class, factory.getSourceClass());
        Object created =
                factory.createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(WooCommerceClientTest.options()),
                                        getClass().getClassLoader()))
                        .createSource();
        WooCommerceSource source = (WooCommerceSource) created;
        assertEquals("WooCommerce", source.getPluginName());
        JobContext context = new JobContext();
        context.setJobMode(JobMode.STREAMING);
        assertThrows(IllegalArgumentException.class, () -> source.setJobContext(context));
    }

    @Test
    void discoversWooCommerceSourceFactory() {
        boolean found = false;
        for (Factory factory : ServiceLoader.load(Factory.class)) {
            found |= factory.factoryIdentifier().equals("WooCommerce");
        }
        assertTrue(found, "WooCommerce source must be discoverable through Factory SPI");
    }
}
