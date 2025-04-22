package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.STABLE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceOptions.USERNAME;

@AutoService(Factory.class)
public class TDengineSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "TDengine";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, USERNAME, PASSWORD, DATABASE, STABLE, LOWER_BOUND, UPPER_BOUND)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TDengineSource.class;
    }
}
