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

package io.debezium.connector.postgresql;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.TemporalPrecisionMode;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class CustomPostgresValueConverter extends PostgresValueConverter {
    protected CustomPostgresValueConverter(
            Charset databaseCharset,
            DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            ZoneOffset defaultOffset,
            BigIntUnsignedMode bigIntUnsignedMode,
            boolean includeUnknownDatatypes,
            TypeRegistry typeRegistry,
            PostgresConnectorConfig.HStoreHandlingMode hStoreMode,
            CommonConnectorConfig.BinaryHandlingMode binaryMode,
            PostgresConnectorConfig.IntervalHandlingMode intervalMode,
            byte[] toastPlaceholder) {
        super(
                databaseCharset,
                decimalMode,
                temporalPrecisionMode,
                defaultOffset,
                bigIntUnsignedMode,
                includeUnknownDatatypes,
                typeRegistry,
                hStoreMode,
                binaryMode,
                intervalMode,
                toastPlaceholder);
    }

    public static CustomPostgresValueConverter of(
            PostgresConnectorConfig connectorConfig,
            Charset databaseCharset,
            TypeRegistry typeRegistry,
            ZoneId zoneId) {
        return new CustomPostgresValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                zoneId.getRules().getOffset(Instant.now()),
                null,
                connectorConfig.includeUnknownDatatypes(),
                typeRegistry,
                connectorConfig.hStoreHandlingMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.intervalHandlingMode(),
                connectorConfig.toastedValuePlaceholder());
    }
}
