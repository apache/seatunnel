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

package org.apache.seatunnel.engine.core.job;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode
public class ConnectorJarIdentifier implements Serializable {

    private byte[] connectorJarID;

    private ConnectorJarType type;

    private String fileName;

    private String storagePath;

    public ConnectorJarIdentifier() {}

    public ConnectorJarIdentifier(ConnectorJarType type, String fileName, String storagePath) {
        this.connectorJarID = new byte[0];
        this.type = type;
        this.fileName = fileName;
        this.storagePath = storagePath;
    }

    public ConnectorJarIdentifier(
            byte[] connectorJarID, ConnectorJarType type, String fileName, String storagePath) {
        this.connectorJarID = connectorJarID;
        this.type = type;
        this.fileName = fileName;
        this.storagePath = storagePath;
    }

    public static ConnectorJarIdentifier of(ConnectorJar connectorJar, String storagePath) {
        ConnectorJarIdentifier connectorJarIdentifier =
                ConnectorJarIdentifier.of(
                        connectorJar.getConnectorJarID(),
                        connectorJar.getType(),
                        connectorJar.getFileName(),
                        storagePath);
        return connectorJarIdentifier;
    }

    public static ConnectorJarIdentifier of(
            ConnectorJarType type, String fileName, String storagePath) {
        return new ConnectorJarIdentifier(type, fileName, storagePath);
    }

    public static ConnectorJarIdentifier of(
            byte[] connectorJarID, ConnectorJarType type, String fileName, String storagePath) {
        return new ConnectorJarIdentifier(connectorJarID, type, fileName, storagePath);
    }
}
