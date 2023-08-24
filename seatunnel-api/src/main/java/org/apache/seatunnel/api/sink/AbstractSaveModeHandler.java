package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;

public abstract class AbstractSaveModeHandler implements AutoCloseable {

    public SchemaSaveMode schemaSaveMode;
    public DataSaveMode dataSaveMode;

    public AbstractSaveModeHandler(SchemaSaveMode schemaSaveMode, DataSaveMode dataSaveMode) {
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
    }

    public void handleSaveMode() {
        handleSchemaSaveMode();
        handleDataSaveMode();
    }

    public void handleSchemaSaveMode() {
        switch (schemaSaveMode) {
            case RECREATE_SCHEMA:
                recreateSchema();
                break;
            case CREATE_SCHEMA_WHEN_NOT_EXIST:
                createSchemaWhenNotExist();
                break;
            case ERROR_WHEN_SCHEMA_NOT_EXIST:
                errorWhenSchemaNotExist();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + schemaSaveMode);
        }
    }

    public void handleDataSaveMode() {
        switch (dataSaveMode) {
            case KEEP_SCHEMA_DROP_DATA:
                keepSchemaDropData();
                break;
            case KEEP_SCHEMA_AND_DATA:
                keepSchemaAndData();
                break;
            case CUSTOM_PROCESSING:
                customProcessing();
                break;
            case ERROR_WHEN_DATA_EXISTS:
                errorWhenDataExists();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + dataSaveMode);
        }
    }

    public void recreateSchema() {
        if (tableExists()) {
            dropTable();
        }
        createTable();
    }

    public void createSchemaWhenNotExist() {
        if (!tableExists()) {
            createTable();
        }
    }

    public void errorWhenSchemaNotExist() {
        if (!tableExists()) {
            throw new SeaTunnelRuntimeException(SINK_TABLE_NOT_EXIST, "The sink table not exist");
        }
    }

    public void keepSchemaDropData() {
        if (tableExists()) {
            truncateTable();
        }
    }

    public void keepSchemaAndData() {}

    public void customProcessing() {
        executeCustomSql();
    }

    public void errorWhenDataExists() {
        if (dataExists()) {
            throw new SeaTunnelRuntimeException(
                    SOURCE_ALREADY_HAS_DATA, "The target data source already has data");
        }
    }

    public abstract boolean tableExists();

    public abstract void dropTable();

    public abstract void createTable();

    public abstract void truncateTable();

    public abstract boolean dataExists();

    public abstract void executeCustomSql();
}
