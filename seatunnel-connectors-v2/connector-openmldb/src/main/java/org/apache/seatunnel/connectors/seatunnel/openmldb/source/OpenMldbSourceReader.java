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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbSqlExecutor;
import org.apache.seatunnel.connectors.seatunnel.openmldb.exception.OpenMldbConnectorException;

import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

@Slf4j
public class OpenMldbSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final OpenMldbParameters openMldbParameters;
    private final SeaTunnelRowType seaTunnelRowType;
    private final SingleSplitReaderContext readerContext;

    public OpenMldbSourceReader(OpenMldbParameters openMldbParameters,
                                SeaTunnelRowType seaTunnelRowType,
                                SingleSplitReaderContext readerContext) {
        this.openMldbParameters = openMldbParameters;
        this.seaTunnelRowType = seaTunnelRowType;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        OpenMldbSqlExecutor.initSdkOption(openMldbParameters);
    }

    @Override
    public void close() throws IOException {
        OpenMldbSqlExecutor.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        int totalFields = seaTunnelRowType.getTotalFields();
        Object[] objects = new Object[totalFields];
        SqlClusterExecutor sqlExecutor = OpenMldbSqlExecutor.getSqlExecutor();
        try (ResultSet resultSet = sqlExecutor.executeSQL(openMldbParameters.getDatabase(),
                openMldbParameters.getSql())) {
            while (resultSet.next()) {
                for (int i = 0; i < totalFields; i++) {
                    objects[i] = getObject(resultSet, i, seaTunnelRowType.getFieldType(i));
                }
                output.collect(new SeaTunnelRow(objects));
            }
        } finally {
            if (Boundedness.BOUNDED.equals(readerContext.getBoundedness())) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded openmldb source");
                readerContext.signalNoMoreElement();
            }
        }
    }

    private Object getObject(ResultSet resultSet, int index, SeaTunnelDataType<?> dataType) throws SQLException {
        index = index + 1;
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return resultSet.getBoolean(index);
            case INT:
                return resultSet.getInt(index);
            case SMALLINT:
                return resultSet.getShort(index);
            case BIGINT:
                return resultSet.getLong(index);
            case FLOAT:
                return resultSet.getFloat(index);
            case DOUBLE:
                return resultSet.getDouble(index);
            case STRING:
                return resultSet.getString(index);
            case DATE:
                Date date = resultSet.getDate(index);
                return date.toLocalDate();
            case TIMESTAMP:
                Timestamp timestamp = resultSet.getTimestamp(index);
                return timestamp.toLocalDateTime();
            default:
                throw new OpenMldbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported this data type");
        }
    }
}
