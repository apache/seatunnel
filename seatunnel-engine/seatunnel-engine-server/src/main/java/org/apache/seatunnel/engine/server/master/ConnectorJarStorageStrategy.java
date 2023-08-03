package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.core.job.ConnectorJar;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;

public interface ConnectorJarStorageStrategy extends Serializable {

    /**
     * Return the path for the connector jar.
     *
     * @param jobId jobId
     * @param connectorJar connectorJar
     * @return the storage path of connector jar
     */
    String getStorageLocationPath(long jobId, ConnectorJar connectorJar);

    /**
     * Return the physical storage location of the connector jar.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJar connector jar
     * @return the (designated) physical storage location of the connector jar
     */
    File getStorageLocation(long jobId, ConnectorJar connectorJar) throws IOException;

    /**
     * storage the connector jar package file.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJar connector jar
     * @return the storage path of connector jar file
     */
    String storageConnectorJarFile(long jobId, ConnectorJar connectorJar);

    Path storageConnectorJarFileInternal(ConnectorJar connectorJar, File storageLocation)
            throws IOException;

    void deleteConnectorJar(long jobId, String connectorJarFileName) throws IOException;

    void deleteConnectorJarInternal(File storageLocation);
}
