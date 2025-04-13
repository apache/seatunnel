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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;

@Data
public class SheetsParameters implements Serializable {

    private byte[] serviceAccountKey;

    private String sheetId;

    private String sheetName;

    private String range;

    public SheetsParameters buildWithConfig(ReadonlyConfig config) {
        this.serviceAccountKey = config.get(SheetsSourceOptions.SERVICE_ACCOUNT_KEY).getBytes();
        this.sheetId = config.get(SheetsSourceOptions.SHEET_ID);
        this.sheetName = config.get(SheetsSourceOptions.SHEET_NAME);
        this.range = config.get(SheetsSourceOptions.RANGE);
        return this;
    }
}
