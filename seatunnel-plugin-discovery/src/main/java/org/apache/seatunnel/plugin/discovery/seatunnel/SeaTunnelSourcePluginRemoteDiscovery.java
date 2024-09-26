package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.plugin.discovery.AbstractPluginRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;

public class SeaTunnelSourcePluginRemoteDiscovery
        extends AbstractPluginRemoteDiscovery<SeaTunnelSource> {

    public SeaTunnelSourcePluginRemoteDiscovery(
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
                                TableSourceFactory.class.isAssignableFrom(pluginFactory.getClass()))
                .forEach(
                        pluginFactory ->
                                getPluginsByFactoryIdentifier(
                                        plugins,
                                        PluginType.SOURCE,
                                        pluginFactory.factoryIdentifier(),
                                        FactoryUtil.sourceFullOptionRule(
                                                (TableSourceFactory) pluginFactory)));
        return plugins;
    }

    @Override
    protected Class<SeaTunnelSource> getPluginBaseClass() {
        return SeaTunnelSource.class;
    }
}
