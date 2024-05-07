package org.apache.seatunnel.transform.groovy;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class GroovyTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return GroovyTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(GroovyTransformConfig.CODE).build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        GroovyTransformConfig groovyTransformConfig =
                GroovyTransformConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        return () -> new GroovyTransform(groovyTransformConfig, catalogTable);
    }
}
