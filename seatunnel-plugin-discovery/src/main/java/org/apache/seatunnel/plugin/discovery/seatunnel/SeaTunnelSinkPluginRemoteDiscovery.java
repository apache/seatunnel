package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.plugin.discovery.AbstractPluginRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;

public class SeaTunnelSinkPluginRemoteDiscovery
        extends AbstractPluginRemoteDiscovery<SeaTunnelSink> {

    private static final String MULTITABLESINK_FACTORYIDENTIFIER = "MultiTableSink";

    public SeaTunnelSinkPluginRemoteDiscovery(
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
                                !pluginFactory
                                                .factoryIdentifier()
                                                .equals(MULTITABLESINK_FACTORYIDENTIFIER)
                                        && TableSinkFactory.class.isAssignableFrom(
                                                pluginFactory.getClass()))
                .forEach(
                        pluginFactory ->
                                getPluginsByFactoryIdentifier(
                                        plugins,
                                        PluginType.SINK,
                                        pluginFactory.factoryIdentifier(),
                                        FactoryUtil.sinkFullOptionRule(
                                                (TableSinkFactory) pluginFactory)));
        return plugins;
    }

    @Override
    protected Class<SeaTunnelSink> getPluginBaseClass() {
        return SeaTunnelSink.class;
    }
}
