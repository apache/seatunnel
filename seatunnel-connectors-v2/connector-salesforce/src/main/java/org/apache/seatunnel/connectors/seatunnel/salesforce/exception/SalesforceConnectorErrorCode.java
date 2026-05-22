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

package org.apache.seatunnel.connectors.seatunnel.salesforce.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum SalesforceConnectorErrorCode implements SeaTunnelErrorCode {
    AUTH_FAILED("SALESFORCE-01", "Failed to authenticate with Salesforce"),
    DESCRIBE_OBJECT_FAILED("SALESFORCE-02", "Failed to describe Salesforce object schema"),
    BULK_JOB_CREATE_FAILED("SALESFORCE-03", "Failed to create Salesforce Bulk API query job"),
    BULK_JOB_FAILED("SALESFORCE-04", "Salesforce Bulk API query job failed or was aborted"),
    BULK_RESULTS_FAILED("SALESFORCE-05", "Failed to retrieve Salesforce Bulk API job results"),
    INVALID_TABLE_PATH("SALESFORCE-06", "Invalid table_path; expected format: database.ObjectName"),
    DUPLICATE_OBJECT("SALESFORCE-07", "Duplicate object found in tables_configs");

    private final String code;
    private final String description;

    SalesforceConnectorErrorCode(String code, String description) {
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
