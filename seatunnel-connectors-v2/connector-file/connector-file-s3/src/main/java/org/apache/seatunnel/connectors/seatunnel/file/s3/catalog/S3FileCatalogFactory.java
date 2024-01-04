package org.apache.seatunnel.connectors.seatunnel.file.s3.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Conf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class S3FileCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        HadoopConf hadoopConf = S3Conf.buildWithReadOnlyConfig(options);
        FileSystemUtils fileSystemUtils = new FileSystemUtils(hadoopConf);
        return new S3FileCatalog(fileSystemUtils, options);
    }

    @Override
    public String factoryIdentifier() {
        return "S3";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }
}
