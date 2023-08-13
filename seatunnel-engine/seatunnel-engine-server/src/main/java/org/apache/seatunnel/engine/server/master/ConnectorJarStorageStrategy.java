/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

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
    File getStorageLocation(long jobId, ConnectorJar connectorJar);

    /**
     * storage the connector jar package file.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJar connector jar
     * @return the storage path of connector jar file
     */
    ConnectorJarIdentifier storageConnectorJarFile(long jobId, ConnectorJar connectorJar);

    Path storageConnectorJarFileInternal(ConnectorJar connectorJar, File storageLocation);

    void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier);

    void deleteConnectorJarInternal(File storageLocation);

    byte[] readConnectorJarByteDataInternal(File connectorJarFile);

    byte[] readConnectorJarByteData(File connectorJarFile);

    void cleanUpWhenJobFinished(List<ConnectorJarIdentifier> connectorJarIdentifierList);
}
