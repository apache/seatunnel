package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import java.util.List;
import java.util.Map;

public class MetaLakeSchemaDiscoverer {

    private TableSourceFactoryContext tableSourceFactoryContext;
    private MetalakeClient metalakeClient;
    private MetaLakeTypeMapper metaLakeTypeMapper;


    public List<CatalogTable> discoverCatalogTables(){
        return null;
    }
}
