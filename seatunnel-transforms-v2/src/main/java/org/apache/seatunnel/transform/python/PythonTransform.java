package org.apache.seatunnel.transform.python;


import lombok.NonNull;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;

public class PythonTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "Python";

    private final PythonTransformConfig config;

    public PythonTransform(@NonNull CatalogTable inputCatalogTable, PythonTransformConfig transformConfig) {
        super(inputCatalogTable, transformConfig.getErrorHandleWay());
        this.config = transformConfig;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        return new Object[0];
    }

    @Override
    protected Column[] getOutputColumns() {
        return new Column[0];
    }
}
