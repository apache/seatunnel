package org.apache.seatunnel.connectors.seatunnel.access.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class AccessSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Access";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AccessConfig.DRIVER, AccessConfig.URL)
                .bundled(AccessConfig.USERNAME, AccessConfig.PASSWORD)
                .optional(AccessConfig.QUERY)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AccessSource.class;
    }
}
