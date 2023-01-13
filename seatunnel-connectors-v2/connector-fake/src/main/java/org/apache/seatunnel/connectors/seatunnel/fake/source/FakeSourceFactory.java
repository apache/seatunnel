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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BYTES_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_DAY_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_MONTH_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_YEAR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.MAP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROWS;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROW_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_READ_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_HOUR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_MINUTE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_SECOND_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_TEMPLATE;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class FakeSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "FakeSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SeaTunnelSchema.SCHEMA)
                .exclusive(STRING_LENGTH, STRING_TEMPLATE)
                .exclusive(TINYINT_MIN, TINYINT_TEMPLATE)
                .exclusive(TINYINT_MAX, TINYINT_TEMPLATE)
                .exclusive(SMALLINT_MIN, SMALLINT_TEMPLATE)
                .exclusive(SMALLINT_MAX, SMALLINT_TEMPLATE)
                .exclusive(INT_MIN, INT_TEMPLATE)
                .exclusive(INT_MAX, INT_TEMPLATE)
                .exclusive(BIGINT_MIN, BIGINT_TEMPLATE)
                .exclusive(BIGINT_MAX, BIGINT_TEMPLATE)
                .exclusive(FLOAT_MIN, FLOAT_TEMPLATE)
                .exclusive(FLOAT_MAX, FLOAT_TEMPLATE)
                .exclusive(DOUBLE_MIN, DOUBLE_TEMPLATE)
                .exclusive(DOUBLE_MAX, DOUBLE_TEMPLATE)
                .optional(
                        ROWS,
                        ROW_NUM,
                        SPLIT_NUM,
                        SPLIT_READ_INTERVAL,
                        MAP_SIZE,
                        ARRAY_SIZE,
                        BYTES_LENGTH,
                        STRING_LENGTH,
                        STRING_TEMPLATE,
                        TINYINT_MIN, TINYINT_MAX, TINYINT_TEMPLATE,
                        SMALLINT_MIN, SMALLINT_MAX, SMALLINT_TEMPLATE,
                        INT_MIN, INT_MAX, INT_TEMPLATE,
                        BIGINT_MIN, BIGINT_MAX, BIGINT_TEMPLATE,
                        FLOAT_MIN, FLOAT_MAX, FLOAT_TEMPLATE,
                        DOUBLE_MIN, DOUBLE_MAX, DOUBLE_TEMPLATE,
                        DATE_YEAR_TEMPLATE, DATE_MONTH_TEMPLATE, DATE_DAY_TEMPLATE,
                        TIME_HOUR_TEMPLATE, TIME_MINUTE_TEMPLATE, TIME_SECOND_TEMPLATE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return FakeSource.class;
    }
}
