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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Getter;

import java.io.Serializable;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.ADVERTISER_ID;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.DATA_LEVEL;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.DIMENSIONS;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.MAX_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.MAX_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.METRICS;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.MOCK_URL;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.REPORT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.RETRY_WAIT;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.tiktok.ads.TikTokAdsSourceOptions.TOKEN;

@Getter
final class TikTokAdsConfig implements Serializable {
    static final String PATH = "/open_api/v1.3/report/integrated/get/";
    private final String token;
    private final String advertiserId;
    private final String startDate;
    private final String endDate;
    private final List<String> dimensions;
    private final List<String> metrics;
    private final String endpoint;
    private final int pageSize;
    private final int maxRows;
    private final int maxBytes;
    private final int timeout;
    private final int reportTimeout;
    private final int retries;
    private final int retryWait;
    private final CatalogTable table;

    TikTokAdsConfig(ReadonlyConfig options) {
        // Do not retain parser causes or input values: either may contain credentials.
        try {
            try {
                ConfigValidator.validateUnknownKeys(
                        options,
                        new TikTokAdsSourceFactory().optionRule(),
                        TikTokAdsSource.PLUGIN_NAME);
            } catch (OptionValidationException e) {
                throw new ValidationException("unsupported option; see TikTokAds options");
            }
            Object parallelism = options.getSourceMap().get("parallelism");
            require(
                    parallelism == null || "1".equals(parallelism.toString()),
                    "parallelism must be 1");
            token = options.get(TOKEN);
            require(
                    token != null && token.matches("[!-~]{1,4096}"),
                    "token must be a nonempty ASCII header value");
            advertiserId = options.get(ADVERTISER_ID);
            require(advertiserId.matches("[0-9]{1,30}"), "advertiser_id must be numeric");
            require("AUCTION_AD".equals(options.get(DATA_LEVEL)), "data_level must be AUCTION_AD");
            startDate = options.get(START_DATE);
            endDate = options.get(END_DATE);
            require(!date(startDate).isAfter(date(endDate)), "start_date must not exceed end_date");
            dimensions = Collections.unmodifiableList(new ArrayList<>(options.get(DIMENSIONS)));
            require(
                    dimensions.equals(Collections.singletonList("ad_id"))
                            || dimensions.equals(Arrays.asList("ad_id", "stat_time_day")),
                    "dimensions must be [ad_id] or [ad_id, stat_time_day]");
            long days = ChronoUnit.DAYS.between(date(startDate), date(endDate)) + 1;
            require(
                    days <= (dimensions.contains("stat_time_day") ? 30 : 365),
                    "date range must be at most 30 inclusive days with stat_time_day or 365 without it");
            metrics = Collections.unmodifiableList(new ArrayList<>(options.get(METRICS)));
            require(
                    !metrics.isEmpty()
                            && new HashSet<>(metrics).size() == metrics.size()
                            && Arrays.asList("spend", "impressions", "clicks").containsAll(metrics),
                    "metrics must be a nonempty unique subset of spend, impressions, clicks");
            String mock = options.getOptional(MOCK_URL).orElse(null);
            if (mock == null) {
                require(!"mock-token".equals(token), "mock-token requires mock_url");
                endpoint = "https://business-api.tiktok.com" + PATH;
            } else {
                require("mock-token".equals(token), "mock_url requires token mock-token");
                URI uri = URI.create(mock);
                require(
                        "http".equals(uri.getScheme())
                                && uri.getHost() != null
                                && uri.getRawUserInfo() == null
                                && uri.getRawQuery() == null
                                && uri.getRawFragment() == null
                                && (uri.getPath().isEmpty() || "/".equals(uri.getPath())),
                        "mock_url must be an HTTP origin without credentials, path, query or fragment");
                endpoint = mock.replaceAll("/$", "") + PATH;
            }
            pageSize = bounded(options.get(PAGE_SIZE), 1, 1000);
            maxRows = bounded(options.get(MAX_ROWS), 1, 1000000);
            maxBytes = bounded(options.get(MAX_BYTES), 1024, 16777216);
            timeout = bounded(options.get(TIMEOUT), 100, 120000);
            reportTimeout = bounded(options.get(REPORT_TIMEOUT), 100, 3600000);
            retries = bounded(options.get(RETRIES), 0, 5);
            retryWait = bounded(options.get(RETRY_WAIT), 1000, 120000);
            table = CatalogTableUtil.buildWithConfig(options);
            validateSchema(table.getSeaTunnelRowType());
        } catch (ValidationException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("TikTokAds: missing or invalid configuration");
        }
    }

    private void validateSchema(SeaTunnelRowType rowType) {
        Set<String> expected = new HashSet<>(dimensions);
        expected.addAll(metrics);
        require(
                rowType.getTotalFields() == expected.size()
                        && expected.equals(new HashSet<>(Arrays.asList(rowType.getFieldNames()))),
                "schema must match all requested dimensions and metrics exactly");
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String name = rowType.getFieldName(i);
            SeaTunnelDataType<?> type = rowType.getFieldType(i);
            if (dimensions.contains(name)) {
                require(BasicType.STRING_TYPE.equals(type), "dimensions require STRING");
            } else if ("spend".equals(name)) {
                require(type instanceof DecimalType, "spend requires DECIMAL");
                DecimalType decimal = (DecimalType) type;
                require(
                        decimal.getPrecision() >= 1
                                && decimal.getPrecision() <= 38
                                && decimal.getScale() >= 0
                                && decimal.getScale() <= decimal.getPrecision(),
                        "invalid DECIMAL precision or scale");
            } else {
                require(BasicType.LONG_TYPE.equals(type), "impressions and clicks require BIGINT");
            }
        }
    }

    static LocalDate date(String value) {
        try {
            require(value != null && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"), "invalid date");
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new ValidationException("invalid calendar date");
        }
    }

    private static int bounded(int value, int min, int max) {
        require(value >= min && value <= max, "numeric option outside allowed bounds");
        return value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new ValidationException(message);
        }
    }

    private static final class ValidationException extends IllegalArgumentException {
        private ValidationException(String message) {
            super("TikTokAds: " + message);
        }
    }
}
