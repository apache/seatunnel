package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.plugin.discovery.AbstractPluginRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;

public class SeaTunnelTransformPluginRemoteDiscovery
        extends AbstractPluginRemoteDiscovery<SeaTunnelTransform> {

    public SeaTunnelTransformPluginRemoteDiscovery(
            Collection<URL> provideLibUrls,
            Config pluginMappingConfig,
            BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer) {
        super(provideLibUrls, pluginMappingConfig, addURLToClassLoaderConsumer);
    }

    @Override
    public ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> getOptionRules(
            String pluginIdentifier) {
        return super.getOptionRules(pluginIdentifier);
    }

    @Override
    public LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {
        LinkedHashMap<PluginIdentifier, OptionRule> plugins = new LinkedHashMap<>();
        getPluginFactories().stream()
                .filter(
                        pluginFactory ->
                                TableTransformFactory.class.isAssignableFrom(
                                        pluginFactory.getClass()))
                .forEach(
                        pluginFactory ->
                                getPluginsByFactoryIdentifier(
                                        plugins,
                                        PluginType.TRANSFORM,
                                        pluginFactory.factoryIdentifier(),
                                        pluginFactory.optionRule()));
        return plugins;
    }

    @Override
    protected Class<SeaTunnelTransform> getPluginBaseClass() {
        return SeaTunnelTransform.class;
    }
}
