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

package org.apache.seatunnel.connectors.bigquery.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.json.JSONArray;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.protobuf.Descriptors;

import java.io.IOException;

public interface BigQueryWriter {
    ApiFuture<AppendRowsResponse> append(JSONArray jsonArr)
            throws Descriptors.DescriptorValidationException, IOException;

    default void onAppendSuccess(int rowCount) {}

    /** Returns whether the underlying JSON stream writer can no longer accept appends. */
    default boolean isClosed() {
        return false;
    }

    void close();

    String getStreamName();

    /**
     * Recreates the JSON stream writer after a table schema change.
     *
     * <p>Implementations must preserve the logical stream and its append position.
     */
    default BigQueryWriter refreshSchema(BigQueryWriteClient client, ReadonlyConfig config) {
        throw new UnsupportedOperationException(
                "This BigQuery writer does not support refreshing its table schema.");
    }
}
