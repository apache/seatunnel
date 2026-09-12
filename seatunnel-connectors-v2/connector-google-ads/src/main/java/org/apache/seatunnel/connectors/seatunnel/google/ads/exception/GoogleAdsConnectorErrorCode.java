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

package org.apache.seatunnel.connectors.seatunnel.google.ads.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum GoogleAdsConnectorErrorCode implements SeaTunnelErrorCode {
    AUTH_FAILED("GOOGLE_ADS-01", "Failed to obtain Google OAuth2 access token"),
    DESCRIBE_FIELDS_FAILED("GOOGLE_ADS-02", "Failed to resolve Google Ads field metadata"),
    SEARCH_FAILED("GOOGLE_ADS-03", "Google Ads search request failed"),
    INVALID_QUERY("GOOGLE_ADS-04", "Invalid GAQL query or fields configuration"),
    INVALID_TABLE_PATH("GOOGLE_ADS-05", "Invalid table_path; expected format: database.resource"),
    DUPLICATE_RESOURCE("GOOGLE_ADS-06", "Duplicate table found in tables_configs");

    private final String code;
    private final String description;

    GoogleAdsConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
