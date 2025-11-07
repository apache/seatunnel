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

package org.apache.seatunnel.connectors.sensorsdata.sdk.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.sensorsdata.format.config.TargetColumnConfig;
import org.apache.seatunnel.connectors.sensorsdata.format.record.RowAccessor;
import org.apache.seatunnel.connectors.sensorsdata.format.record.SensorsDataRecordBuilder;
import org.apache.seatunnel.connectors.sensorsdata.format.record.SpecialItemRecord;
import org.apache.seatunnel.connectors.sensorsdata.format.record.UserDetailRecord;
import org.apache.seatunnel.connectors.sensorsdata.format.record.UserEventRecord;
import org.apache.seatunnel.connectors.sensorsdata.format.record.UserRecord;
import org.apache.seatunnel.connectors.sensorsdata.format.utils.UserSchemaUtil;
import org.apache.seatunnel.connectors.sensorsdata.sdk.config.SensorsDataSDKSinkConfig;
import org.apache.seatunnel.connectors.sensorsdata.sdk.exception.SensorsDataConnectorErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.sdk.exception.SensorsDataConnectorException;
import org.apache.seatunnel.connectors.sensorsdata.sdk.state.SensorsDataCommitInfo;
import org.apache.seatunnel.connectors.sensorsdata.sdk.state.SensorsDataSinkState;

import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConsoleConsumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SensorsDataSDKWriter
        implements SinkWriter<SeaTunnelRow, SensorsDataCommitInfo, SensorsDataSinkState>,
                SupportMultiTableSinkWriter<Void> {

    private final SensorsAnalytics sa;
    private final RowAccessor rowAccessor;
    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean isSkipErrorRecord;
    private final boolean nullAsProfileUnset;
    private final Set<String> allProperties;

    /** for convenient testing */
    private static final String CONSUMER_TYPE_CONSOLE = "console";

    private final SensorsDataRecordBuilder.Builder recordBuilder;

    public SensorsDataSDKWriter(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            @NonNull SensorsDataSDKSinkConfig sinkConfig) {
        if (CONSUMER_TYPE_CONSOLE.equalsIgnoreCase(sinkConfig.getConsumer())) {
            sa = new SensorsAnalytics(new ConsoleConsumer(new PrintWriter(System.out)));
        } else {
            sa =
                    new SensorsAnalytics(
                            new BatchConsumer(
                                    sinkConfig.getServerUrl(),
                                    sinkConfig.getBulkSize(),
                                    sinkConfig.getMaxCacheRowSize(),
                                    false,
                                    3,
                                    sinkConfig.getInstantEvents()));
        }
        sa.setEnableTimeFree(sinkConfig.isTimeFree());
        rowAccessor = new RowAccessor(sinkConfig, seaTunnelRowType);
        this.seaTunnelRowType = seaTunnelRowType;
        isSkipErrorRecord = sinkConfig.isSkipErrorRecord();
        nullAsProfileUnset = sinkConfig.isNullAsProfileUnset();
        recordBuilder = SensorsDataRecordBuilder.newBuilder(sinkConfig, rowAccessor);
        this.allProperties =
                sinkConfig.getPropertyFields().stream()
                        .map(TargetColumnConfig::getTarget)
                        .collect(Collectors.toSet());
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        try {
            switch (recordBuilder.getRecordType()) {
                case USER:
                    UserSchema userSchema = ((UserRecord) recordBuilder.build(row)).getUserSchema();
                    sa.profileSet(userSchema);
                    if (nullAsProfileUnset) {
                        UserSchema unsetUserSchema =
                                UserSchemaUtil.buildUnsetUserSchema(userSchema, allProperties);
                        if (unsetUserSchema != null) {
                            // do not send profile_unset if all fields are not null
                            sa.profileUnset(unsetUserSchema);
                        }
                    }
                    break;
                case USER_EVENT:
                    sa.track(((UserEventRecord) recordBuilder.build(row)).getUserEventSchema());
                    break;
                case USER_DETAIL:
                    sa.detailSet(
                            ((UserDetailRecord) recordBuilder.build(row)).getUserDetailSchema());
                    break;
                case SPECIAL_ITEM:
                    sa.itemSet(((SpecialItemRecord) recordBuilder.build(row)).getItemRecord());
                    break;
                default:
                    throw new SensorsDataConnectorException(
                            SensorsDataConnectorErrorCode.UNSUPPORTED_RECORD_TYPE,
                            "Unsupported record type");
            }
        } catch (Exception e) {
            log.error("Write error", e);
            log.error(
                    "Write error, SeaTunnelRow#tableId={} SeaTunnelRow#kind={} : [{}]",
                    row.getTableId(),
                    row.getRowKind(),
                    fieldsToString(row));
            if (!isSkipErrorRecord) {
                throw new SensorsDataConnectorException(
                        SensorsDataConnectorErrorCode.SEND_RECORD_FAILED, e.getMessage(), e);
            }
        }
    }

    /** Convert the SeaTunnelRow data to a string */
    private String fieldsToString(SeaTunnelRow row) {
        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            arr[i] = fieldToString(fieldTypes[i], fields[i]);
        }
        return StringUtils.join(arr, ", ");
    }

    /** copy from ConsoleSinkWriter */
    private String fieldToString(SeaTunnelDataType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                List<String> arrayData = new ArrayList<>();
                for (int i = 0; i < Array.getLength(value); i++) {
                    arrayData.add(String.valueOf(Array.get(value, i)));
                }
                return arrayData.toString();
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                List<String> rowData = new ArrayList<>();
                SeaTunnelRowType rowType = (SeaTunnelRowType) type;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    rowData.add(
                            fieldToString(
                                    rowType.getFieldTypes()[i],
                                    ((SeaTunnelRow) value).getField(i)));
                }
                return rowData.toString();
            default:
                return String.valueOf(value);
        }
    }

    @Override
    public Optional<SensorsDataCommitInfo> prepareCommit() throws IOException {
        sa.flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}
}
