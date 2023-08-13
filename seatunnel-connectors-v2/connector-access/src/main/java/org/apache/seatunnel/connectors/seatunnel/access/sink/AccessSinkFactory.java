package org.apache.seatunnel.connectors.seatunnel.access.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class AccessSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Access";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AccessConfig.DRIVER, AccessConfig.URL, AccessConfig.TABLE)
                .bundled(AccessConfig.USERNAME, AccessConfig.PASSWORD)
                .optional(AccessConfig.QUERY)
                .build();
    }
}
