package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;

public class DorisCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new DorisCatalog(
                catalogName,
                options.get(DorisOptions.FENODES),
                options.get(DorisOptions.QUERY_PORT),
                options.get(DorisOptions.USERNAME),
                options.get(DorisOptions.PASSWORD),
                DorisConfig.of(options),
                options.get(DorisOptions.DEFAULT_DATABASE));
    }

    @Override
    public String factoryIdentifier() {
        return "Doris";
    }

    @Override
    public OptionRule optionRule() {
        return DorisOptions.CATALOG_RULE.build();
    }
}
