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

package org.apache.seatunnel.connectors.seatunnel.google.firestore.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.config.FirestoreParameters;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.exception.FirestoreConnectorException;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.google.firestore.config.FirestoreConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.google.firestore.config.FirestoreConfig.PROJECT_ID;

@AutoService(SeaTunnelSink.class)
public class FirestoreSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private SeaTunnelRowType rowType;

    private FirestoreParameters firestoreParameters;

    @Override
    public String getPluginName() {
        return "GoogleFirestore";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(pluginConfig, PROJECT_ID.key(), COLLECTION.key());
        if (!result.isSuccess()) {
            throw new FirestoreConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        this.firestoreParameters = new FirestoreParameters().buildWithConfig(pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.rowType = seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new FirestoreSinkWriter(rowType, firestoreParameters);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return super.getWriteCatalogTable();
    }
}
