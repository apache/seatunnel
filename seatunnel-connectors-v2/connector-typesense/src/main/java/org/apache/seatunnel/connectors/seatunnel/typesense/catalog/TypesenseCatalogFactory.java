package org.apache.seatunnel.connectors.seatunnel.typesense.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class TypesenseCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new TypesenseCatalog(catalogName, "", options);
    }

    @Override
    public String factoryIdentifier() {
        return "Typesense";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }
}
