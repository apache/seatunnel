package org.apache.seatunnel.connectors.seatunnel.file.oss.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.oss.config.OssHadoopConf;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class OssFileCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        HadoopConf hadoopConf = OssHadoopConf.buildWithConfig(options);
        HadoopFileSystemProxy fileSystemUtils = new HadoopFileSystemProxy(hadoopConf);
        return new OssFileCatalog(
                fileSystemUtils,
                options.get(BaseSourceConfigOptions.FILE_PATH),
                FileSystemType.OSS.getFileSystemPluginName());
    }

    @Override
    public String factoryIdentifier() {
        return FileSystemType.OSS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }
}
