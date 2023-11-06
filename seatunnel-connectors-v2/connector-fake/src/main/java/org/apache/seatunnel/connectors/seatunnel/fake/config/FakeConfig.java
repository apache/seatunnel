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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.fake.exception.FakeConnectorException;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_TEMPLATE;
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

    @Builder.Default private List<TableIdentifier> tableIdentifiers = new ArrayList<>();

    // todo: use ReadonlyConfig
    public static FakeConfig buildWithConfig(Config config) {
        FakeConfigBuilder builder = FakeConfig.builder();
        if (config.hasPath(ROW_NUM.key())) {
            builder.rowNum(config.getInt(ROW_NUM.key()));
        }
        if (config.hasPath(SPLIT_NUM.key())) {
            builder.splitNum(config.getInt(SPLIT_NUM.key()));
        }
        if (config.hasPath(SPLIT_READ_INTERVAL.key())) {
            builder.splitReadInterval(config.getInt(SPLIT_READ_INTERVAL.key()));
        }
        if (config.hasPath(MAP_SIZE.key())) {
            builder.mapSize(config.getInt(MAP_SIZE.key()));
        }
        if (config.hasPath(ARRAY_SIZE.key())) {
            builder.arraySize(config.getInt(ARRAY_SIZE.key()));
        }
        if (config.hasPath(BYTES_LENGTH.key())) {
            builder.bytesLength(config.getInt(BYTES_LENGTH.key()));
        }
        if (config.hasPath(STRING_LENGTH.key())) {
            builder.stringLength(config.getInt(STRING_LENGTH.key()));
        }
        if (config.hasPath(ROWS.key())) {
            List<? extends Config> configs = config.getConfigList(ROWS.key());
            List<RowData> rows = new ArrayList<>(configs.size());
            ConfigRenderOptions options = ConfigRenderOptions.concise();
            for (Config configItem : configs) {
                String fieldsJson = configItem.getValue(RowData.KEY_FIELDS).render(options);
                RowData rowData = new RowData(configItem.getString(RowData.KEY_KIND), fieldsJson);
                rows.add(rowData);
            }
            builder.fakeRows(rows);
        }
        if (config.hasPath(STRING_TEMPLATE.key())) {
            builder.stringTemplate(config.getStringList(STRING_TEMPLATE.key()));
        }
        if (config.hasPath(TINYINT_TEMPLATE.key())) {
            builder.tinyintTemplate(config.getIntList(TINYINT_TEMPLATE.key()));
        }
        if (config.hasPath(SMALLINT_TEMPLATE.key())) {
            builder.smallintTemplate(config.getIntList(SMALLINT_TEMPLATE.key()));
        }
        if (config.hasPath(INT_TEMPLATE.key())) {
            builder.intTemplate(config.getIntList(INT_TEMPLATE.key()));
        }
        if (config.hasPath(BIGINT_TEMPLATE.key())) {
            builder.bigTemplate(config.getLongList(BIGINT_TEMPLATE.key()));
        }
        if (config.hasPath(FLOAT_TEMPLATE.key())) {
            builder.floatTemplate(config.getDoubleList(FLOAT_TEMPLATE.key()));
        }
        if (config.hasPath(DOUBLE_TEMPLATE.key())) {
            builder.doubleTemplate(config.getDoubleList(DOUBLE_TEMPLATE.key()));
        }
        if (config.hasPath(DATE_YEAR_TEMPLATE.key())) {
            builder.dateYearTemplate(config.getIntList(DATE_YEAR_TEMPLATE.key()));
        }
        if (config.hasPath(DATE_MONTH_TEMPLATE.key())) {
            builder.dateMonthTemplate(config.getIntList(DATE_MONTH_TEMPLATE.key()));
        }
        if (config.hasPath(DATE_DAY_TEMPLATE.key())) {
            builder.dateDayTemplate(config.getIntList(DATE_DAY_TEMPLATE.key()));
        }
        if (config.hasPath(TIME_HOUR_TEMPLATE.key())) {
            builder.timeHourTemplate(config.getIntList(TIME_HOUR_TEMPLATE.key()));
        }
        if (config.hasPath(TIME_MINUTE_TEMPLATE.key())) {
            builder.timeMinuteTemplate(config.getIntList(TIME_MINUTE_TEMPLATE.key()));
        }
        if (config.hasPath(TIME_SECOND_TEMPLATE.key())) {
            builder.timeSecondTemplate(config.getIntList(TIME_SECOND_TEMPLATE.key()));
        }
        if (config.hasPath(TINYINT_MIN.key())) {
            int tinyintMin = config.getInt(TINYINT_MIN.key());
            if (tinyintMin < TINYINT_MIN.defaultValue()
                    || tinyintMin > TINYINT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        TINYINT_MIN.key()
                                + " should >= "
                                + TINYINT_MIN.defaultValue()
                                + " and <= "
                                + TINYINT_MAX.defaultValue());
            }
            builder.tinyintMin(tinyintMin);
        }
        if (config.hasPath(TINYINT_MAX.key())) {
            int tinyintMax = config.getInt(TINYINT_MAX.key());
            if (tinyintMax < TINYINT_MIN.defaultValue()
                    || tinyintMax > TINYINT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        TINYINT_MAX.key()
                                + " should >= "
                                + TINYINT_MIN.defaultValue()
                                + " and <= "
                                + TINYINT_MAX.defaultValue());
            }
            builder.tinyintMax(tinyintMax);
        }
        if (config.hasPath(SMALLINT_MIN.key())) {
            int smallintMin = config.getInt(SMALLINT_MIN.key());
            if (smallintMin < SMALLINT_MIN.defaultValue()
                    || smallintMin > SMALLINT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        SMALLINT_MIN.key()
                                + " should >= "
                                + SMALLINT_MIN.defaultValue()
                                + " and <= "
                                + SMALLINT_MAX.defaultValue());
            }
            builder.smallintMin(smallintMin);
        }
        if (config.hasPath(SMALLINT_MAX.key())) {
            int smallintMax = config.getInt(SMALLINT_MAX.key());
            if (smallintMax < SMALLINT_MIN.defaultValue()
                    || smallintMax > SMALLINT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        SMALLINT_MAX.key()
                                + " should >= "
                                + SMALLINT_MIN.defaultValue()
                                + " and <= "
                                + SMALLINT_MAX.defaultValue());
            }
            builder.smallintMax(smallintMax);
        }
        if (config.hasPath(INT_MIN.key())) {
            int intMin = config.getInt(INT_MIN.key());
            if (intMin < INT_MIN.defaultValue() || intMin > INT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        INT_MIN.key()
                                + " should >= "
                                + INT_MIN.defaultValue()
                                + " and <= "
                                + INT_MAX.defaultValue());
            }
            builder.intMin(intMin);
        }
        if (config.hasPath(INT_MAX.key())) {
            int intMax = config.getInt(INT_MAX.key());
            if (intMax < INT_MIN.defaultValue() || intMax > INT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        INT_MAX.key()
                                + " should >= "
                                + INT_MIN.defaultValue()
                                + " and <= "
                                + INT_MAX.defaultValue());
            }
            builder.intMax(intMax);
        }
        if (config.hasPath(BIGINT_MIN.key())) {
            long bigintMin = config.getLong(BIGINT_MIN.key());
            if (bigintMin < BIGINT_MIN.defaultValue() || bigintMin > BIGINT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        BIGINT_MIN.key()
                                + " should >= "
                                + BIGINT_MIN.defaultValue()
                                + " and <= "
                                + BIGINT_MAX.defaultValue());
            }
            builder.bigintMin(bigintMin);
        }
        if (config.hasPath(BIGINT_MAX.key())) {
            long bigintMax = config.getLong(BIGINT_MAX.key());
            if (bigintMax < BIGINT_MIN.defaultValue() || bigintMax > BIGINT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        BIGINT_MAX.key()
                                + " should >= "
                                + BIGINT_MIN.defaultValue()
                                + " and <= "
                                + BIGINT_MAX.defaultValue());
            }
            builder.bigintMax(bigintMax);
        }
        if (config.hasPath(FLOAT_MIN.key())) {
            double floatMin = config.getDouble(FLOAT_MIN.key());
            if (floatMin < FLOAT_MIN.defaultValue() || floatMin > FLOAT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        FLOAT_MIN.key()
                                + " should >= "
                                + FLOAT_MIN.defaultValue()
                                + " and <= "
                                + FLOAT_MAX.defaultValue());
            }
            builder.floatMin(floatMin);
        }
        if (config.hasPath(FLOAT_MAX.key())) {
            double floatMax = config.getDouble(FLOAT_MAX.key());
            if (floatMax < FLOAT_MIN.defaultValue() || floatMax > FLOAT_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        FLOAT_MAX.key()
                                + " should >= "
                                + FLOAT_MIN.defaultValue()
                                + " and <= "
                                + FLOAT_MAX.defaultValue());
            }
            builder.floatMax(floatMax);
        }
        if (config.hasPath(DOUBLE_MIN.key())) {
            double doubleMin = config.getDouble(DOUBLE_MIN.key());
            if (doubleMin < DOUBLE_MIN.defaultValue() || doubleMin > DOUBLE_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        DOUBLE_MIN.key()
                                + " should >= "
                                + DOUBLE_MIN.defaultValue()
                                + " and <= "
                                + DOUBLE_MAX.defaultValue());
            }
            builder.doubleMin(doubleMin);
        }
        if (config.hasPath(DOUBLE_MAX.key())) {
            double doubleMax = config.getDouble(DOUBLE_MAX.key());
            if (doubleMax < DOUBLE_MIN.defaultValue() || doubleMax > DOUBLE_MAX.defaultValue()) {
                throw new FakeConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        DOUBLE_MAX.key()
                                + " should >= "
                                + DOUBLE_MIN.defaultValue()
                                + " and <= "
                                + DOUBLE_MAX.defaultValue());
            }
            builder.doubleMax(doubleMax);
        }
        if (config.hasPath(STRING_FAKE_MODE.key())) {
            builder.stringFakeMode(
                    FakeOption.FakeMode.parse(config.getString(STRING_FAKE_MODE.key())));
        }
        if (config.hasPath(TINYINT_FAKE_MODE.key())) {
            builder.tinyintFakeMode(
                    FakeOption.FakeMode.parse(config.getString(TINYINT_FAKE_MODE.key())));
        }
        if (config.hasPath(SMALLINT_FAKE_MODE.key())) {
            builder.smallintFakeMode(
                    FakeOption.FakeMode.parse(config.getString(SMALLINT_FAKE_MODE.key())));
        }
        if (config.hasPath(INT_FAKE_MODE.key())) {
            builder.intFakeMode(FakeOption.FakeMode.parse(config.getString(INT_FAKE_MODE.key())));
        }
        if (config.hasPath(BIGINT_FAKE_MODE.key())) {
            builder.bigintFakeMode(
                    FakeOption.FakeMode.parse(config.getString(BIGINT_FAKE_MODE.key())));
        }
        if (config.hasPath(FLOAT_FAKE_MODE.key())) {
            builder.floatFakeMode(
                    FakeOption.FakeMode.parse(config.getString(FLOAT_FAKE_MODE.key())));
        }
        if (config.hasPath(DOUBLE_FAKE_MODE.key())) {
            builder.doubleFakeMode(
                    FakeOption.FakeMode.parse(config.getString(DOUBLE_FAKE_MODE.key())));
        }
        if (config.hasPath(CatalogOptions.TABLE_NAMES.key())) {
            List<String> tableNames = config.getStringList(CatalogOptions.TABLE_NAMES.key());
            List<TableIdentifier> tableIdentifiers = new ArrayList<>(tableNames.size());
            for (String tableName : tableNames) {
                tableIdentifiers.add(TableIdentifier.of("FakeSource", TablePath.of(tableName)));
            }
            builder.tableIdentifiers(tableIdentifiers);
        }
        return builder.build();
    }

    @Getter
    @AllArgsConstructor
    public static class RowData implements Serializable {
        static final String KEY_KIND = "kind";
        static final String KEY_FIELDS = "fields";

        private String kind;
        private String fieldsJson;
    }
}
