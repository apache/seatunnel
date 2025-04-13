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

package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;

import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;

public class SeaTunnelFactoryDiscovery extends AbstractPluginDiscovery<Factory> {

    private final Class<? extends Factory> factoryClass;

    public SeaTunnelFactoryDiscovery(Class<? extends Factory> factoryClass) {
        super();
        this.factoryClass = factoryClass;
    }

    public SeaTunnelFactoryDiscovery(
            Class<? extends Factory> factoryClass,
            BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super(addURLToClassLoader);
        this.factoryClass = factoryClass;
    }

    @Override
    protected Class<Factory> getPluginBaseClass() {
        return Factory.class;
    }

    @Override
    protected Factory loadPluginInstance(
            PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<Factory> serviceLoader =
                ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (Factory factory : serviceLoader) {
            if (factoryClass.isInstance(factory)) {
                String factoryIdentifier = factory.factoryIdentifier();
                String pluginName = pluginIdentifier.getPluginName();
                if (StringUtils.equalsIgnoreCase(factoryIdentifier, pluginName)) {
                    return factory;
                }
            }
        }
        return null;
    }
}
