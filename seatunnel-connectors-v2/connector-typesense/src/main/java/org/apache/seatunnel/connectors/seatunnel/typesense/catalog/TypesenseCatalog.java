package org.apache.seatunnel.connectors.seatunnel.typesense.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class TypesenseCatalog implements Catalog {

    private final String catalogName;
    private final String defaultDatabase;

    private final ReadonlyConfig config;

    public TypesenseCatalog(String catalogName, String defaultDatabase, ReadonlyConfig config) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.config = checkNotNull(config, "Typesense Config cannot be null");
    }

}
