package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.plugin.discovery.AbstractPluginRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.Collection;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;

public class SeaTunnelFactoryRemoteDiscovery extends AbstractPluginRemoteDiscovery<Factory> {

    private final Class<? extends Factory> factoryClass;

    public SeaTunnelFactoryRemoteDiscovery(
            Collection<URL> provideLibUrls,
            Config pluginMappingConfig,
            BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer,
            Class<? extends Factory> factoryClass) {
        super(provideLibUrls, pluginMappingConfig, addURLToClassLoaderConsumer);
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
