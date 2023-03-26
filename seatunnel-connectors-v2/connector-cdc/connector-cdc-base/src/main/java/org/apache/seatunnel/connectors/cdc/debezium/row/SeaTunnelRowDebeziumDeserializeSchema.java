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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isDataChangeRecord;

/** Deserialization schema from Debezium object to {@link SeaTunnelRow}. */
@Slf4j
public final class SeaTunnelRowDebeziumDeserializeSchema
        implements DebeziumDeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    /** TypeInformation of the produced {@link SeaTunnelRow}. * */
    private final SeaTunnelDataType<SeaTunnelRow> resultTypeInfo;

    /**
     * Runtime converter that converts Kafka {@link SourceRecord}s into {@link SeaTunnelRow}
     * consisted of
     */
    private final SeaTunnelRowDebeziumDeserializationConverters singleTableRowConverter;

    private final Map<String, SeaTunnelRowDebeziumDeserializationConverters>
            multipleTableRowConverters;

    /** Validator to validate the row value. */
    private final ValueValidator validator;

    /** Returns a builder to build {@link SeaTunnelRowDebeziumDeserializeSchema}. */
    public static Builder builder() {
        return new Builder();
    }

    SeaTunnelRowDebeziumDeserializeSchema(
            SeaTunnelDataType<SeaTunnelRow> physicalDataType,
            MetadataConverter[] metadataConverters,
            SeaTunnelDataType<SeaTunnelRow> resultType,
            ValueValidator validator,
            ZoneId serverTimeZone,
            DebeziumDeserializationConverterFactory userDefinedConverterFactory) {

        SeaTunnelRowDebeziumDeserializationConverters singleTableRowConverter = null;
        Map<String, SeaTunnelRowDebeziumDeserializationConverters> multipleTableRowConverters =
                Collections.emptyMap();
        if (physicalDataType instanceof MultipleRowType) {
            multipleTableRowConverters = new HashMap<>();
            for (Map.Entry<String, SeaTunnelRowType> item : (MultipleRowType) physicalDataType) {
                SeaTunnelRowDebeziumDeserializationConverters itemRowConverter =
                        new SeaTunnelRowDebeziumDeserializationConverters(
                                item.getValue(),
                                metadataConverters,
                                serverTimeZone,
                                userDefinedConverterFactory);
                multipleTableRowConverters.put(item.getKey(), itemRowConverter);
            }
        } else {
            singleTableRowConverter =
                    new SeaTunnelRowDebeziumDeserializationConverters(
                            (SeaTunnelRowType) physicalDataType,
                            metadataConverters,
                            serverTimeZone,
                            userDefinedConverterFactory);
        }
        this.singleTableRowConverter = singleTableRowConverter;
        this.multipleTableRowConverters = multipleTableRowConverters;
        this.resultTypeInfo = checkNotNull(resultType);
        this.validator = checkNotNull(validator);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<SeaTunnelRow> collector)
            throws Exception {
        if (!isDataChangeRecord(record)) {
            log.debug("Unsupported record {}, just skip.", record);
            return;
        }

        Envelope.Operation operation = Envelope.operationFor(record);
        Struct messageStruct = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
        String databaseName = sourceStruct.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
        String tableName = sourceStruct.getString(AbstractSourceInfo.TABLE_NAME_KEY);
        String schemaName = null;
        try {
            schemaName = sourceStruct.getString(AbstractSourceInfo.SCHEMA_NAME_KEY);
        } catch (Throwable e) {
            // ignore
        }
        String tableId = TablePath.of(databaseName, schemaName, tableName).toString();
        SeaTunnelRowDebeziumDeserializationConverters converters;
        if (!multipleTableRowConverters.isEmpty()) {
            converters = multipleTableRowConverters.get(tableId);
            if (converters == null) {
                log.debug("Ignore newly added table {}", tableId);
                return;
            }
        } else {
            converters = singleTableRowConverter;
        }

        if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
            SeaTunnelRow insert = extractAfterRow(converters, record, messageStruct, valueSchema);
            insert.setRowKind(RowKind.INSERT);
            insert.setTableId(tableId);
            validator.validate(insert, RowKind.INSERT);
            collector.collect(insert);
        } else if (operation == Envelope.Operation.DELETE) {
            SeaTunnelRow delete = extractBeforeRow(converters, record, messageStruct, valueSchema);
            validator.validate(delete, RowKind.DELETE);
            delete.setRowKind(RowKind.DELETE);
            delete.setTableId(tableId);
            collector.collect(delete);
        } else {
            SeaTunnelRow before = extractBeforeRow(converters, record, messageStruct, valueSchema);
            validator.validate(before, RowKind.UPDATE_BEFORE);
            before.setRowKind(RowKind.UPDATE_BEFORE);
            before.setTableId(tableId);
            collector.collect(before);

            SeaTunnelRow after = extractAfterRow(converters, record, messageStruct, valueSchema);
            validator.validate(after, RowKind.UPDATE_AFTER);
            after.setRowKind(RowKind.UPDATE_AFTER);
            after.setTableId(tableId);
            collector.collect(after);
        }
    }

    private SeaTunnelRow extractAfterRow(
            SeaTunnelRowDebeziumDeserializationConverters runtimeConverter,
            SourceRecord record,
            Struct value,
            Schema valueSchema)
            throws Exception {

        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return runtimeConverter.convert(record, after, afterSchema);
    }

    private SeaTunnelRow extractBeforeRow(
            SeaTunnelRowDebeziumDeserializationConverters runtimeConverter,
            SourceRecord record,
            Struct value,
            Schema valueSchema)
            throws Exception {

        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return runtimeConverter.convert(record, before, beforeSchema);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return resultTypeInfo;
    }

    // -------------------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------------------

    /** Custom validator to validate the row value. */
    public interface ValueValidator extends Serializable {
        void validate(SeaTunnelRow rowData, RowKind rowKind) throws Exception;
    }

    /** Builder of {@link SeaTunnelRowDebeziumDeserializeSchema}. */
    public static class Builder {
        private SeaTunnelDataType<SeaTunnelRow> physicalRowType;
        private SeaTunnelDataType<SeaTunnelRow> resultTypeInfo;
        private MetadataConverter[] metadataConverters = new MetadataConverter[0];
        private ValueValidator validator = (rowData, rowKind) -> {};
        private ZoneId serverTimeZone = ZoneId.of("UTC");
        private DebeziumDeserializationConverterFactory userDefinedConverterFactory =
                DebeziumDeserializationConverterFactory.DEFAULT;

        public Builder setPhysicalRowType(SeaTunnelDataType<SeaTunnelRow> physicalRowType) {
            this.physicalRowType = physicalRowType;
            return this;
        }

        public Builder setMetadataConverters(MetadataConverter[] metadataConverters) {
            this.metadataConverters = metadataConverters;
            return this;
        }

        public Builder setResultTypeInfo(SeaTunnelDataType<SeaTunnelRow> resultTypeInfo) {
            this.resultTypeInfo = resultTypeInfo;
            return this;
        }

        public Builder setValueValidator(ValueValidator validator) {
            this.validator = validator;
            return this;
        }

        public Builder setServerTimeZone(ZoneId serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder setUserDefinedConverterFactory(
                DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
            this.userDefinedConverterFactory = userDefinedConverterFactory;
            return this;
        }

        public SeaTunnelRowDebeziumDeserializeSchema build() {
            return new SeaTunnelRowDebeziumDeserializeSchema(
                    physicalRowType,
                    metadataConverters,
                    resultTypeInfo,
                    validator,
                    serverTimeZone,
                    userDefinedConverterFactory);
        }
    }
}
