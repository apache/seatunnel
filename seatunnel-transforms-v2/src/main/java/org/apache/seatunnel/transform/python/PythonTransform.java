package org.apache.seatunnel.transform.python;


import lombok.NonNull;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;

public class PythonTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "Python";

    private final PythonTransformConfig transformConfig;

    private final PythonOperationProxy pythonOperationProxy;

    public PythonTransform(@NonNull CatalogTable inputCatalogTable, PythonTransformConfig transformConfig) {
        super(inputCatalogTable, transformConfig.getErrorHandleWay());
        this.transformConfig = transformConfig;
        this.pythonOperationProxy = initLocalSingletonJavaServer(transformConfig);
    }

    private PythonOperationProxy initLocalSingletonJavaServer(PythonTransformConfig transformConfig) {
        return PythonOperationProxy.newInstance(transformConfig);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        long threadId = Thread.currentThread().getId();
        return pythonOperationProxy.processData(
                threadId,
                inputRow
        );
    }

    @Override
    protected Column[] getOutputColumns() {
        return this.transformConfig.getColumnConfigs().stream()
                .map(PythonColumnConfig::getDestColumn)
                .toArray(Column[]::new);
    }
}
