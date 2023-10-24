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
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

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
     * Storage the connector jar package file.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJar connector jar
     * @return the storage path of connector jar file
     */
    ConnectorJarIdentifier storageConnectorJarFile(long jobId, ConnectorJar connectorJar);

    /**
     * Storage the connector jar package file in the local file system.
     *
     * @param connectorJar connector jar
     * @param storageLocation the storage location of the connector jar in the local file system
     * @return
     */
    Optional<Path> storageConnectorJarFileInternal(ConnectorJar connectorJar, File storageLocation);

    /**
     * Check whether the same connector Jar package exists in the zeta engine.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJar connector jar
     * @return
     */
    boolean checkConnectorJarExisted(long jobId, ConnectorJar connectorJar);

    /**
     * Obtain the unique identifier of the connector jar.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJar connector jar
     * @return
     */
    ConnectorJarIdentifier getConnectorJarIdentifier(long jobId, ConnectorJar connectorJar);

    /**
     * Delete the connector jar package by connectorJarIdentifier.
     *
     * @param connectorJarIdentifier the unique identifier of the connector jar.
     */
    void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier);

    /**
     * Delete the connector jar package in execution node by connectorJarIdentifier
     *
     * @param connectorJarIdentifier the unique identifier of the connector jar.
     */
    void deleteConnectorJarInExecutionNode(ConnectorJarIdentifier connectorJarIdentifier);

    /**
     * Delete the connector jar package by connectorJarIdentifier
     *
     * @param connectorJarIdentifier the unique identifier of the connector jar.
     */
    /**
     * Delete the connector jar package in the local file system by connectorJarIdentifier.
     *
     * @param storageLocation the storage location of the connector jar
     */
    void deleteConnectorJarInternal(File storageLocation);

    /**
     * Read connector Jar package from file to byte array.
     *
     * @param connectorJarFile the connector jar file
     * @return the byte array of the connector jar file
     */
    byte[] readConnectorJarByteDataInternal(File connectorJarFile);

    /**
     * Read connector Jar package from file to byte array.
     *
     * @param connectorJarFile the connector jar file
     * @return the byte array of the connector jar file
     */
    byte[] readConnectorJarByteData(File connectorJarFile);

    /**
     * Carry out the cleaning work after the task is finished.
     *
     * @param jobId ID of the job for the connector jar
     * @param connectorJarIdentifierList List of all Jar package identifiers referenced by the
     *     current task
     */
    void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList);
}
