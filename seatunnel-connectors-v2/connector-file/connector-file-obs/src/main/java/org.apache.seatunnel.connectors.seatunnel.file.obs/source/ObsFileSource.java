package org.apache.seatunnel.connectors.seatunnel.obs.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.obs.config.ObsConf;
import org.apache.seatunnel.connectors.seatunnel.obs.config.ObsConfig;
import org.apache.seatunnel.connectors.seatunnel.obs.exception.ObsConnectorException;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSource.class)
public class ObsFileSource extends BaseFileSource {
    @Override
    public String getPluginName() {
        return "ObsFile";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        ObsConfig.FILE_PATH.key(),
                        ObsConfig.FILE_FORMAT_TYPE.key(),
                        ObsConfig.ENDPOINT.key(),
                        ObsConfig.ACCESS_KEY.key(),
                        ObsConfig.SECURITY_KEY.key(),
                        ObsConfig.BUCKET.key());
        if (!result.isSuccess()) {
            throw new ObsConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        readStrategy =
                ReadStrategyFactory.of(pluginConfig.getString(ObsConfig.FILE_FORMAT_TYPE.key()));
        readStrategy.setPluginConfig(pluginConfig);
        String path = pluginConfig.getString(ObsConfig.FILE_PATH.key());
        hadoopConf = ObsConf.buildWithConfig(pluginConfig);
        try {
            filePaths = readStrategy.getFileNamesByPath(hadoopConf, path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
        // support user-defined schema
        FileFormat fileFormat =
                FileFormat.valueOf(
                        pluginConfig.getString(ObsConfig.FILE_FORMAT_TYPE.key()).toUpperCase());
        // only json text csv type support user-defined schema now
        if (pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key())) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                case EXCEL:
                    SeaTunnelRowType userDefinedSchema =
                            CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
                    readStrategy.setSeaTunnelRowTypeInfo(userDefinedSchema);
                    rowType = readStrategy.getActualSeaTunnelRowTypeInfo();
                    break;
                case ORC:
                case PARQUET:
                    throw new ObsConnectorException(
                            CommonErrorCode.UNSUPPORTED_OPERATION,
                            "SeaTunnel does not support user-defined schema for [parquet, orc] files");
                default:
                    // never got in there
                    throw new ObsConnectorException(
                            CommonErrorCode.UNSUPPORTED_OPERATION,
                            "SeaTunnel does not supported this file format");
            }
        } else {
            try {
                rowType = readStrategy.getSeaTunnelRowTypeInfo(hadoopConf, filePaths.get(0));
            } catch (FileConnectorException e) {
                String errorMsg =
                        String.format("Get table schema from file [%s] failed", filePaths.get(0));
                throw new FileConnectorException(
                        CommonErrorCode.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
            }
        }
    }
}
