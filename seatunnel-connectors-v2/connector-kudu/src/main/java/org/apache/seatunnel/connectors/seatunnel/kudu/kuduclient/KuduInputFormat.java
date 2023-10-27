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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.source.KuduSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.kudu.util.KuduUtil;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.api.table.type.SqlType.TIMESTAMP;

@Slf4j
public class KuduInputFormat implements Serializable {

    private final KuduSourceConfig kuduSourceConfig;
    private final SeaTunnelRowType rowTypeInfo;

    /** Declare the global variable KuduClient and use it to manipulate the Kudu table */
    public KuduClient kuduClient;

    public KuduInputFormat(
            @NonNull KuduSourceConfig kuduSourceConfig, SeaTunnelRowType rowTypeInfo) {
        this.kuduSourceConfig = kuduSourceConfig;
        this.rowTypeInfo = rowTypeInfo;
    }

    public void openInputFormat() {
        if (kuduClient == null) {
            kuduClient = KuduUtil.getKuduClient(kuduSourceConfig);
        }
    }

    public SeaTunnelRow toInternal(RowResult rs) throws SQLException {
        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = rowTypeInfo.getFieldTypes();
        for (int i = 0; i < seaTunnelDataTypes.length; i++) {
            if (seaTunnelDataTypes[i].getSqlType() == TIMESTAMP) {
                Timestamp timestamp = rs.getTimestamp(i);
                fields.add(
                        Optional.ofNullable(timestamp).map(e -> e.toLocalDateTime()).orElse(null));
                continue;
            }
            fields.add(rs.getObject(i));
        }
        return new SeaTunnelRow(fields.toArray());
    }

    public void closeInputFormat() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
            } catch (KuduException e) {
                throw new KuduConnectorException(
                        KuduConnectorErrorCode.CLOSE_KUDU_CLIENT_FAILED, e);
            } finally {
                kuduClient = null;
            }
        }
    }

    public Set<KuduSourceSplit> createInputSplits() throws IOException {
        List<KuduScanToken> scanTokens =
                KuduUtil.getKuduScanToken(kuduSourceConfig, rowTypeInfo.getFieldNames());
        Set<KuduSourceSplit> allSplit = new HashSet<>(scanTokens.size());
        for (int i = 0; i < scanTokens.size(); i++) {
            allSplit.add(new KuduSourceSplit(i, scanTokens.get(i).serialize()));
        }
        return allSplit;
    }

    public KuduScanner scanner(byte[] token) throws IOException {
        return KuduScanToken.deserializeIntoScanner(token, kuduClient);
    }
}
