package org.apache.seatunnel.connectors.seatunnel.file.s3.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.LocatedFileStatus;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.List;

@AllArgsConstructor
public class S3FileCatalog implements Catalog {

    private final FileSystemUtils fileSystemUtils;
    private final ReadonlyConfig readonlyConfig;

    @Override
    public void open() throws CatalogException {}

    @Override
    public void close() throws CatalogException {}

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return null;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        return null;
    }

    @SneakyThrows
    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return fileSystemUtils.existsPath(readonlyConfig.get(S3Config.FILE_PATH));
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return null;
    }

    @SneakyThrows
    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        fileSystemUtils.createDir(readonlyConfig.get(S3Config.FILE_PATH));
    }

    @SneakyThrows
    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        fileSystemUtils.deleteDir(readonlyConfig.get(S3Config.FILE_PATH));
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {}

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {}

    @SneakyThrows
    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (fileSystemUtils.deleteDir(readonlyConfig.get(S3Config.FILE_PATH))) {
            fileSystemUtils.createDir(readonlyConfig.get(S3Config.FILE_PATH));
        }
    }

    @SneakyThrows
    @Override
    public boolean isExistsData(TablePath tablePath) {
        final List<LocatedFileStatus> locatedFileStatuses =
                fileSystemUtils.fileList(readonlyConfig.get(S3Config.FILE_PATH));
        return CollectionUtils.isNotEmpty(locatedFileStatuses);
    }
}
