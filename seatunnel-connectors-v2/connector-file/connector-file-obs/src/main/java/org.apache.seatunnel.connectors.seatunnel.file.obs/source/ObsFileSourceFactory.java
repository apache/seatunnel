package org.apache.seatunnel.connectors.seatunnel.obs.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.obs.config.ObsConfig;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class ObsFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "ObsFile";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ObsConfig.FILE_PATH)
                .required(ObsConfig.BUCKET)
                .required(ObsConfig.ACCESS_KEY)
                .required(ObsConfig.SECURITY_KEY)
                .required(ObsConfig.ENDPOINT)
                .required(ObsConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSourceConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSourceConfig.DELIMITER)
                .conditional(
                        BaseSourceConfig.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.EXCEL, FileFormat.CSV),
                        CatalogTableUtil.SCHEMA)
                .optional(BaseSourceConfig.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfig.DATE_FORMAT)
                .optional(BaseSourceConfig.DATETIME_FORMAT)
                .optional(BaseSourceConfig.TIME_FORMAT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ObsFileSource.class;
    }
}
