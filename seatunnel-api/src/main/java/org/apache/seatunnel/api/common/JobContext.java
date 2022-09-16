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

package org.apache.seatunnel.api.common;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.JobMode;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to store the context of the job. e.g. the table schema, catalog...etc.
 */
public final class JobContext implements Serializable {

    private static final long serialVersionUID = -1L;

    // tableName -> tableSchema
    private final Map<String, TableSchema> tableSchemaMap = new ConcurrentHashMap<>(Common.COLLECTION_SIZE);

    private JobMode jobMode;

    private final String jobId;

    public JobContext() {
        this.jobId = UUID.randomUUID().toString().replace("-", "");
    }

    public JobContext(Long jobId) {
        this.jobId = jobId + "";
    }

    /**
     * Put table schema.
     *
     * @param tableName   table name
     * @param tableSchema table schema
     */
    public void addSchema(String tableName, TableSchema tableSchema) {
        tableSchemaMap.put(tableName, tableSchema);
    }

    /**
     * Get table schema.
     *
     * @param tableName table name.
     * @return table schema.
     */
    public Optional<TableSchema> getSchema(String tableName) {
        return Optional.ofNullable(tableSchemaMap.get(tableName));
    }

    public JobContext setJobMode(JobMode jobMode) {
        this.jobMode = jobMode;
        return this;
    }

    public JobMode getJobMode() {
        return jobMode;
    }

    public String getJobId() {
        return this.jobId;
    }

}
