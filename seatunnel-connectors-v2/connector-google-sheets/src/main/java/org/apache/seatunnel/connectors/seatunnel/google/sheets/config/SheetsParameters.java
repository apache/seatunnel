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

import org.apache.seatunnel.connectors.seatunnel.google.sheets.exception.GoogleSheetsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.exception.GoogleSheetsConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;
import lombok.Data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Collections;

@Data
public class SheetsParameters implements Serializable {

    private static final String APPLICATION_NAME = "SeaTunnel Google Sheets";

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    private byte[] serviceAccountKey;

    private String sheetId;

    private String sheetName;

    private String range;

    public SheetsParameters buildWithConfig(Config config) {
        this.serviceAccountKey =
                config.getString(SheetsConfig.SERVICE_ACCOUNT_KEY.key()).getBytes();
        this.sheetId = config.getString(SheetsConfig.SHEET_ID.key());
        this.sheetName = config.getString(SheetsConfig.SHEET_NAME.key());
        this.range = config.getString(SheetsConfig.RANGE.key());
        return this;
    }

    public Sheets buildSheets() throws IOException {
        byte[] keyBytes = Base64.getDecoder().decode(this.serviceAccountKey);
        ServiceAccountCredentials sourceCredentials = ServiceAccountCredentials
                .fromStream(new ByteArrayInputStream(keyBytes));
        sourceCredentials = (ServiceAccountCredentials) sourceCredentials
                .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS));
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(sourceCredentials);
        NetHttpTransport httpTransport = null;
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException e) {
            throw new GoogleSheetsConnectorException(GoogleSheetsConnectorErrorCode.BUILD_SHEETS_REQUEST_EXCEPTION,
                    "Build google sheets http request exception", e);
        }
        return new Sheets.Builder(httpTransport, JSON_FACTORY, requestInitializer)
                .setApplicationName(APPLICATION_NAME)
                .build();

    }
}
