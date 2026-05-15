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

import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DynamicMetadataDataSource represents a datasource configuration stored in Hazelcast IMap. It
 * allows users to register datasource connection properties through REST API and reference them in
 * job configs by metadata_datasource_id.
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class DynamicMetadataDataSource implements IdentifiedDataSerializable {

    /** Unique datasource identifier */
    private String metadataDatasourceId;

    /** Datasource or connector type (e.g., jdbc, mysql-cdc, postgres-cdc, kafka) */
    private String connectorType;

    /** Connector-specific connection properties */
    private Map<String, Object> properties;

    /** Timestamp when the datasource was created */
    private Long createTime;

    /** Timestamp when the datasource was last updated */
    private Long updateTime;

    public DynamicMetadataDataSource(
            String datasourceId, String type, Map<String, Object> properties) {
        this.metadataDatasourceId = datasourceId;
        this.connectorType = type;
        this.properties = properties != null ? properties : new HashMap<>();
        long now = System.currentTimeMillis();
        this.createTime = now;
        this.updateTime = now;
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.METADATA_DATASOURCE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(metadataDatasourceId);
        out.writeString(connectorType);
        out.writeObject(properties == null ? new HashMap<>() : new HashMap<>(properties));
        out.writeLong(createTime);
        out.writeLong(updateTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        metadataDatasourceId = in.readString();
        connectorType = in.readString();
        properties = in.readObject();
        createTime = in.readLong();
        updateTime = in.readLong();
    }

    /**
     * Update the properties and set the update time to current time.
     *
     * @param newProperties the new properties to merge/update
     */
    public void updateProperties(Map<String, Object> newProperties) {
        if (newProperties != null) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.putAll(newProperties);
            this.updateTime = System.currentTimeMillis();
        }
    }
}
