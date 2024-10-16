package org.apache.seatunnel.connectors.seatunnel.milvus.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class MilvusCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new MilvusCatalog(catalogName, options);
    }

    @Override
    public String factoryIdentifier() {
        return "Milvus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }
}
