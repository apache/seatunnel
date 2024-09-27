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

package org.apache.seatunnel.connectors.seatunnel.fake.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.fake.exception.FakeConnectorException;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BINARY_VECTOR_DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BYTES_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_DAY_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_MONTH_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_YEAR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.MAP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROWS;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROW_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_READ_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_HOUR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_MINUTE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_SECOND_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.VECTOR_DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.VECTOR_FLOAT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.VECTOR_FLOAT_MIN;

@Builder
@Getter
public class FakeConfig implements Serializable {
    @Builder.Default private int rowNum = ROW_NUM.defaultValue();

    @Builder.Default private int splitNum = SPLIT_NUM.defaultValue();

    @Builder.Default private int splitReadInterval = SPLIT_READ_INTERVAL.defaultValue();

    @Builder.Default private int mapSize = MAP_SIZE.defaultValue();

    @Builder.Default private int arraySize = ARRAY_SIZE.defaultValue();

    @Builder.Default private int bytesLength = BYTES_LENGTH.defaultValue();

    @Builder.Default private int stringLength = STRING_LENGTH.defaultValue();

    @Builder.Default private int tinyintMin = TINYINT_MIN.defaultValue();

    @Builder.Default private int tinyintMax = TINYINT_MAX.defaultValue();

    @Builder.Default private int smallintMin = SMALLINT_MIN.defaultValue();

    @Builder.Default private int smallintMax = SMALLINT_MAX.defaultValue();

    @Builder.Default private int intMin = INT_MIN.defaultValue();

    @Builder.Default private int intMax = INT_MAX.defaultValue();

    @Builder.Default private long bigintMin = BIGINT_MIN.defaultValue();

    @Builder.Default private long bigintMax = BIGINT_MAX.defaultValue();

    @Builder.Default private double floatMin = FLOAT_MIN.defaultValue();

    @Builder.Default private double floatMax = FLOAT_MAX.defaultValue();

    @Builder.Default private double doubleMin = DOUBLE_MIN.defaultValue();

    @Builder.Default private double doubleMax = DOUBLE_MAX.defaultValue();

    @Builder.Default private float vectorFloatMin = VECTOR_FLOAT_MIN.defaultValue();

    @Builder.Default private float vectorFloatMax = VECTOR_FLOAT_MAX.defaultValue();

    @Builder.Default private int vectorDimension = VECTOR_DIMENSION.defaultValue();

    @Builder.Default private int binaryVectorDimension = BINARY_VECTOR_DIMENSION.defaultValue();

    @Builder.Default private FakeOption.FakeMode stringFakeMode = STRING_FAKE_MODE.defaultValue();

    @Builder.Default private FakeOption.FakeMode tinyintFakeMode = TINYINT_FAKE_MODE.defaultValue();

    @Builder.Default
    private FakeOption.FakeMode smallintFakeMode = SMALLINT_FAKE_MODE.defaultValue();

    @Builder.Default private FakeOption.FakeMode intFakeMode = INT_FAKE_MODE.defaultValue();

    @Builder.Default private FakeOption.FakeMode bigintFakeMode = BIGINT_FAKE_MODE.defaultValue();

    @Builder.Default private FakeOption.FakeMode floatFakeMode = FLOAT_FAKE_MODE.defaultValue();

    @Builder.Default private FakeOption.FakeMode doubleFakeMode = DOUBLE_FAKE_MODE.defaultValue();

    private List<String> stringTemplate;
    private List<Integer> tinyintTemplate;
    private List<Integer> smallintTemplate;
    private List<Integer> intTemplate;
    private List<Long> bigTemplate;
    private List<Double> floatTemplate;
    private List<Double> doubleTemplate;

    private List<Integer> dateYearTemplate;
    private List<Integer> dateMonthTemplate;
    private List<Integer> dateDayTemplate;

    private List<Integer> timeHourTemplate;
    private List<Integer> timeMinuteTemplate;
    private List<Integer> timeSecondTemplate;

    private List<RowData> fakeRows;

    private CatalogTable catalogTable;

    public static FakeConfig buildWithConfig(ReadonlyConfig readonlyConfig) {
        FakeConfigBuilder builder = FakeConfig.builder();
        builder.rowNum(readonlyConfig.get(ROW_NUM));
        builder.splitNum(readonlyConfig.get(SPLIT_NUM));
        builder.splitReadInterval(readonlyConfig.get(SPLIT_READ_INTERVAL));
        builder.mapSize(readonlyConfig.get(MAP_SIZE));
        builder.arraySize(readonlyConfig.get(ARRAY_SIZE));
        builder.vectorDimension(readonlyConfig.get(VECTOR_DIMENSION));
        builder.binaryVectorDimension(readonlyConfig.get(BINARY_VECTOR_DIMENSION));
        builder.bytesLength(readonlyConfig.get(BYTES_LENGTH));
        builder.stringLength(readonlyConfig.get(STRING_LENGTH));

        if (readonlyConfig.getOptional(ROWS).isPresent()) {
            List<Map<String, Object>> configs = readonlyConfig.get(ROWS);
            List<RowData> rows = new ArrayList<>(configs.size());
            for (Map<String, Object> configItem : configs) {
                String fieldsJson = JsonUtils.toJsonString(configItem.get(RowData.KEY_FIELDS));
                RowData rowData =
                        new RowData(configItem.get(RowData.KEY_KIND).toString(), fieldsJson);
                rows.add(rowData);
            }
            builder.fakeRows(rows);
        }
        readonlyConfig.getOptional(STRING_TEMPLATE).ifPresent(builder::stringTemplate);
        readonlyConfig.getOptional(TINYINT_TEMPLATE).ifPresent(builder::tinyintTemplate);
        readonlyConfig.getOptional(SMALLINT_TEMPLATE).ifPresent(builder::smallintTemplate);
        readonlyConfig.getOptional(INT_TEMPLATE).ifPresent(builder::intTemplate);
        readonlyConfig.getOptional(BIGINT_TEMPLATE).ifPresent(builder::bigTemplate);
        readonlyConfig.getOptional(FLOAT_TEMPLATE).ifPresent(builder::floatTemplate);
        readonlyConfig.getOptional(DOUBLE_TEMPLATE).ifPresent(builder::doubleTemplate);
        readonlyConfig.getOptional(DATE_YEAR_TEMPLATE).ifPresent(builder::dateYearTemplate);
        readonlyConfig.getOptional(DATE_MONTH_TEMPLATE).ifPresent(builder::dateMonthTemplate);
        readonlyConfig.getOptional(DATE_DAY_TEMPLATE).ifPresent(builder::dateDayTemplate);
        readonlyConfig.getOptional(TIME_HOUR_TEMPLATE).ifPresent(builder::timeHourTemplate);
        readonlyConfig.getOptional(TIME_MINUTE_TEMPLATE).ifPresent(builder::timeMinuteTemplate);
        readonlyConfig.getOptional(TIME_SECOND_TEMPLATE).ifPresent(builder::timeSecondTemplate);

        readonlyConfig
                .getOptional(TINYINT_MIN)
                .ifPresent(
                        tinyintMin -> {
                            if (tinyintMin < TINYINT_MIN.defaultValue()
                                    || tinyintMin > TINYINT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        TINYINT_MIN.key()
                                                + " should >= "
                                                + TINYINT_MIN.defaultValue()
                                                + " and <= "
                                                + TINYINT_MAX.defaultValue());
                            }
                            builder.tinyintMin(tinyintMin);
                        });

        readonlyConfig
                .getOptional(TINYINT_MAX)
                .ifPresent(
                        tinyintMax -> {
                            if (tinyintMax < TINYINT_MIN.defaultValue()
                                    || tinyintMax > TINYINT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        TINYINT_MAX.key()
                                                + " should >= "
                                                + TINYINT_MIN.defaultValue()
                                                + " and <= "
                                                + TINYINT_MAX.defaultValue());
                            }
                            builder.tinyintMax(tinyintMax);
                        });

        readonlyConfig
                .getOptional(SMALLINT_MIN)
                .ifPresent(
                        smallintMin -> {
                            if (smallintMin < SMALLINT_MIN.defaultValue()
                                    || smallintMin > SMALLINT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        SMALLINT_MIN.key()
                                                + " should >= "
                                                + SMALLINT_MIN.defaultValue()
                                                + " and <= "
                                                + SMALLINT_MAX.defaultValue());
                            }
                            builder.smallintMin(smallintMin);
                        });

        readonlyConfig
                .getOptional(SMALLINT_MAX)
                .ifPresent(
                        smallintMax -> {
                            if (smallintMax < SMALLINT_MIN.defaultValue()
                                    || smallintMax > SMALLINT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        SMALLINT_MAX.key()
                                                + " should >= "
                                                + SMALLINT_MIN.defaultValue()
                                                + " and <= "
                                                + SMALLINT_MAX.defaultValue());
                            }
                            builder.smallintMax(smallintMax);
                        });

        readonlyConfig
                .getOptional(INT_MIN)
                .ifPresent(
                        intMin -> {
                            if (intMin < INT_MIN.defaultValue()
                                    || intMin > INT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        INT_MIN.key()
                                                + " should >= "
                                                + INT_MIN.defaultValue()
                                                + " and <= "
                                                + INT_MAX.defaultValue());
                            }
                            builder.intMin(intMin);
                        });

        readonlyConfig
                .getOptional(INT_MAX)
                .ifPresent(
                        intMax -> {
                            if (intMax < INT_MIN.defaultValue()
                                    || intMax > INT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        INT_MAX.key()
                                                + " should >= "
                                                + INT_MIN.defaultValue()
                                                + " and <= "
                                                + INT_MAX.defaultValue());
                            }
                            builder.intMax(intMax);
                        });

        readonlyConfig
                .getOptional(BIGINT_MIN)
                .ifPresent(
                        bigintMin -> {
                            if (bigintMin < BIGINT_MIN.defaultValue()
                                    || bigintMin > BIGINT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        BIGINT_MIN.key()
                                                + " should >= "
                                                + BIGINT_MIN.defaultValue()
                                                + " and <= "
                                                + BIGINT_MAX.defaultValue());
                            }
                            builder.bigintMin(bigintMin);
                        });

        readonlyConfig
                .getOptional(BIGINT_MAX)
                .ifPresent(
                        bigintMax -> {
                            if (bigintMax < BIGINT_MIN.defaultValue()
                                    || bigintMax > BIGINT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        BIGINT_MAX.key()
                                                + " should >= "
                                                + BIGINT_MIN.defaultValue()
                                                + " and <= "
                                                + BIGINT_MAX.defaultValue());
                            }
                            builder.bigintMax(bigintMax);
                        });

        readonlyConfig
                .getOptional(FLOAT_MIN)
                .ifPresent(
                        floatMin -> {
                            if (floatMin < FLOAT_MIN.defaultValue()
                                    || floatMin > FLOAT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        FLOAT_MIN.key()
                                                + " should >= "
                                                + FLOAT_MIN.defaultValue()
                                                + " and <= "
                                                + FLOAT_MAX.defaultValue());
                            }
                            builder.floatMin(floatMin);
                        });

        readonlyConfig
                .getOptional(FLOAT_MAX)
                .ifPresent(
                        floatMax -> {
                            if (floatMax < FLOAT_MIN.defaultValue()
                                    || floatMax > FLOAT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        FLOAT_MAX.key()
                                                + " should >= "
                                                + FLOAT_MIN.defaultValue()
                                                + " and <= "
                                                + FLOAT_MAX.defaultValue());
                            }
                            builder.floatMax(floatMax);
                        });

        readonlyConfig
                .getOptional(DOUBLE_MIN)
                .ifPresent(
                        doubleMin -> {
                            if (doubleMin < DOUBLE_MIN.defaultValue()
                                    || doubleMin > DOUBLE_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        DOUBLE_MIN.key()
                                                + " should >= "
                                                + DOUBLE_MIN.defaultValue()
                                                + " and <= "
                                                + DOUBLE_MAX.defaultValue());
                            }
                            builder.doubleMin(doubleMin);
                        });

        readonlyConfig
                .getOptional(DOUBLE_MAX)
                .ifPresent(
                        doubleMax -> {
                            if (doubleMax < DOUBLE_MIN.defaultValue()
                                    || doubleMax > DOUBLE_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        DOUBLE_MAX.key()
                                                + " should >= "
                                                + DOUBLE_MIN.defaultValue()
                                                + " and <= "
                                                + DOUBLE_MAX.defaultValue());
                            }
                            builder.doubleMax(doubleMax);
                        });

        readonlyConfig
                .getOptional(VECTOR_FLOAT_MIN)
                .ifPresent(
                        vectorFloatMin -> {
                            if (vectorFloatMin < VECTOR_FLOAT_MIN.defaultValue()
                                    || vectorFloatMin > VECTOR_FLOAT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        VECTOR_FLOAT_MIN.key()
                                                + " should >= "
                                                + VECTOR_FLOAT_MIN.defaultValue()
                                                + " and <= "
                                                + VECTOR_FLOAT_MAX.defaultValue());
                            }
                            builder.vectorFloatMin(vectorFloatMin);
                        });

        readonlyConfig
                .getOptional(VECTOR_FLOAT_MAX)
                .ifPresent(
                        vectorFloatMax -> {
                            if (vectorFloatMax < VECTOR_FLOAT_MIN.defaultValue()
                                    || vectorFloatMax > VECTOR_FLOAT_MAX.defaultValue()) {
                                throw new FakeConnectorException(
                                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                        VECTOR_FLOAT_MAX.key()
                                                + " should >= "
                                                + VECTOR_FLOAT_MIN.defaultValue()
                                                + " and <= "
                                                + VECTOR_FLOAT_MAX.defaultValue());
                            }
                            builder.vectorFloatMax(vectorFloatMax);
                        });

        readonlyConfig.getOptional(STRING_FAKE_MODE).ifPresent(builder::stringFakeMode);
        readonlyConfig.getOptional(TINYINT_FAKE_MODE).ifPresent(builder::tinyintFakeMode);
        readonlyConfig.getOptional(SMALLINT_FAKE_MODE).ifPresent(builder::smallintFakeMode);
        readonlyConfig.getOptional(INT_FAKE_MODE).ifPresent(builder::intFakeMode);
        readonlyConfig.getOptional(BIGINT_FAKE_MODE).ifPresent(builder::bigintFakeMode);
        readonlyConfig.getOptional(FLOAT_FAKE_MODE).ifPresent(builder::floatFakeMode);
        readonlyConfig.getOptional(DOUBLE_FAKE_MODE).ifPresent(builder::doubleFakeMode);

        builder.catalogTable(CatalogTableUtil.buildWithConfig("FakeSource", readonlyConfig));

        return builder.build();
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class RowData implements Serializable {
        static final String KEY_KIND = "kind";
        static final String KEY_FIELDS = "fields";

        private String kind;
        private String fieldsJson;
    }
}
