package org.apache.seatunnel.connectors.seatunnel.file.oss.catalog;

import org.apache.seatunnel.connectors.seatunnel.file.catalog.AbstractFileCatalog;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

public class OssFileCatalog extends AbstractFileCatalog {
    public OssFileCatalog(
            HadoopFileSystemProxy hadoopFileSystemProxy, String filePath, String catalogName) {
        super(hadoopFileSystemProxy, filePath, catalogName);
    }
}
