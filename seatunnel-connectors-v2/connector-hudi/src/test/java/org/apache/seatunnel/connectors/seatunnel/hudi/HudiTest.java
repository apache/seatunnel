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

package org.apache.seatunnel.connectors.seatunnel.hudi;

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.convertToSchema;
import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.RowDataToAvroConverters.createConverter;

public class HudiTest {

    protected static @TempDir java.nio.file.Path tempDir;
    private static final String tableName = "hudi";

    protected static final String DEFAULT_PARTITION_PATH = "default";
    public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
    protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
    protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

    private static final String recordKeyFields = "int";

    private static final String partitionFields = "date";

    private static final SeaTunnelRowType seaTunnelRowType =
            new SeaTunnelRowType(
                    new String[] {
                        "bool",
                        "int",
                        "longValue",
                        "float",
                        "name",
                        "date",
                        "time",
                        "timestamp3",
                        "map",
                        "decimal"
                    },
                    new SeaTunnelDataType[] {
                        BOOLEAN_TYPE,
                        INT_TYPE,
                        LONG_TYPE,
                        FLOAT_TYPE,
                        STRING_TYPE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_TIME_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        new MapType(STRING_TYPE, LONG_TYPE),
                        new DecimalType(10, 5),
                    });

    private String getSchema() {
        return convertToSchema(
                        seaTunnelRowType, AvroSchemaUtils.getAvroRecordQualifiedName(tableName))
                .toString();
    }

    @Test
    void testSchema() {
        Assertions.assertEquals(
                "{\"type\":\"record\",\"name\":\"hudi_record\",\"namespace\":\"hoodie.hudi\",\"fields\":[{\"name\":\"bool\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"int\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"longValue\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"float\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"date\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"time-millis\"}],\"default\":null},{\"name\":\"timestamp3\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"long\"]}],\"default\":null},{\"name\":\"decimal\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.hudi.hudi_record.decimal\",\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":5}],\"default\":null}]}",
                getSchema());
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void testWriteData() throws IOException {
        String tablePath = tempDir.toString();
        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName(tableName)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(new HadoopStorageConfiguration(new Configuration()), tablePath);

        HoodieWriteConfig cfg =
                HoodieWriteConfig.newBuilder()
                        .withPath(tablePath)
                        .withSchema(getSchema())
                        .withParallelism(2, 2)
                        .withDeleteParallelism(2)
                        .forTable(tableName)
                        .withIndexConfig(
                                HoodieIndexConfig.newBuilder()
                                        .withIndexType(HoodieIndex.IndexType.INMEMORY)
                                        .build())
                        .withArchivalConfig(
                                HoodieArchivalConfig.newBuilder()
                                        .archiveCommitsWith(11, 25)
                                        .build())
                        .withAutoCommit(false)
                        .build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> javaWriteClient =
                new HoodieJavaWriteClient<>(
                        new HoodieJavaEngineContext(
                                new HadoopStorageConfiguration(new Configuration())),
                        cfg)) {
            SeaTunnelRow expected = new SeaTunnelRow(12);
            Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
            expected.setField(0, true);
            expected.setField(1, 45536);
            expected.setField(2, 1238123899121L);
            expected.setField(3, 33.333F);
            expected.setField(4, "asdlkjasjkdla998y1122");
            expected.setField(5, LocalDate.parse("1990-10-14"));
            expected.setField(6, LocalTime.parse("12:12:43"));
            expected.setField(7, timestamp3.toLocalDateTime());
            Map<String, Long> map = new HashMap<>();
            map.put("element", 123L);
            expected.setField(8, map);
            expected.setField(9, BigDecimal.valueOf(10.121));
            String instantTime = javaWriteClient.startCommit();
            List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = new ArrayList<>();
            hoodieRecords.add(convertRow(expected));
            List<WriteStatus> insert = javaWriteClient.insert(hoodieRecords, instantTime);

            javaWriteClient.commit(instantTime, insert);
        }
    }

    private HoodieRecord<HoodieAvroPayload> convertRow(SeaTunnelRow element) {
        GenericRecord rec =
                new GenericData.Record(
                        new Schema.Parser()
                                .parse(
                                        convertToSchema(
                                                        seaTunnelRowType,
                                                        AvroSchemaUtils.getAvroRecordQualifiedName(
                                                                tableName))
                                                .toString()));
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            rec.put(
                    seaTunnelRowType.getFieldNames()[i],
                    createConverter(seaTunnelRowType.getFieldType(i))
                            .convert(
                                    convertToSchema(
                                            seaTunnelRowType.getFieldType(i),
                                            AvroSchemaUtils.getAvroRecordQualifiedName(tableName)
                                                    + "."
                                                    + seaTunnelRowType.getFieldNames()[i]),
                                    element.getField(i)));
        }

        return new HoodieAvroRecord<>(
                getHoodieKey(element, seaTunnelRowType), new HoodieAvroPayload(Option.of(rec)));
    }

    private HoodieKey getHoodieKey(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        String partitionPath = getRecordPartitionPath(element, seaTunnelRowType);
        String rowKey = getRecordKey(element, seaTunnelRowType);
        return new HoodieKey(rowKey, partitionPath);
    }

    private String getRecordKey(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        boolean keyIsNullEmpty = true;
        StringBuilder recordKey = new StringBuilder();
        for (String recordKeyField : recordKeyFields.split(",")) {
            String recordKeyValue =
                    getNestedFieldValAsString(element, seaTunnelRowType, recordKeyField);
            recordKeyField = recordKeyField.toLowerCase();
            if (recordKeyValue == null) {
                recordKey
                        .append(recordKeyField)
                        .append(":")
                        .append(NULL_RECORDKEY_PLACEHOLDER)
                        .append(",");
            } else if (recordKeyValue.isEmpty()) {
                recordKey
                        .append(recordKeyField)
                        .append(":")
                        .append(EMPTY_RECORDKEY_PLACEHOLDER)
                        .append(",");
            } else {
                recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(",");
                keyIsNullEmpty = false;
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        if (keyIsNullEmpty) {
            throw new HoodieKeyException(
                    "recordKey values: \""
                            + recordKey
                            + "\" for fields: "
                            + recordKeyFields
                            + " cannot be entirely null or empty.");
        }
        return recordKey.toString();
    }

    private String getRecordPartitionPath(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {

        StringBuilder partitionPath = new StringBuilder();
        String[] avroPartitionPathFields = partitionFields.split(",");
        for (String partitionPathField : avroPartitionPathFields) {
            String fieldVal =
                    getNestedFieldValAsString(element, seaTunnelRowType, partitionPathField);
            if (fieldVal == null || fieldVal.isEmpty()) {
                partitionPath.append(partitionPathField).append("=").append(DEFAULT_PARTITION_PATH);
            } else {
                partitionPath.append(partitionPathField).append("=").append(fieldVal);
            }
            partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
        }
        partitionPath.deleteCharAt(partitionPath.length() - 1);
        return partitionPath.toString();
    }

    private String getNestedFieldValAsString(
            SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType, String fieldName) {
        Object value = null;

        if (Arrays.stream(seaTunnelRowType.getFieldNames())
                .collect(Collectors.toList())
                .contains(fieldName)) {
            value = element.getField(seaTunnelRowType.indexOf(fieldName));
        }
        return StringUtils.objToString(value);
    }
}
