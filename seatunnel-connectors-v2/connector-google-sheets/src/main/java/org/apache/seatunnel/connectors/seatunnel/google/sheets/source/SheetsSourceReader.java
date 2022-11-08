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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.deserialize.GoogleSheetsDeserializer;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.deserialize.SeaTunnelRowDeserializer;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class SheetsSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private SheetsParameters sheetsParameters;

    private SeaTunnelRowType seaTunnelRowType;

    private HttpRequestInitializer requestInitializer;

    private static final String APPLICATION_NAME = "SeaTunnel Google Sheets";

    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    private final SingleSplitReaderContext context;


    private final SeaTunnelRowDeserializer seaTunnelRowDeserializer;

    public SheetsSourceReader(SheetsParameters sheetsParameters, SingleSplitReaderContext context, DeserializationSchema<SeaTunnelRow> deserializationSchema, SeaTunnelRowType seaTunnelRowType) throws IOException {
        this.sheetsParameters = sheetsParameters;
        this.context = context;
        this.seaTunnelRowType = seaTunnelRowType;
        this.seaTunnelRowDeserializer = new GoogleSheetsDeserializer(seaTunnelRowType.getFieldNames(), deserializationSchema);
    }

    @Override
    public void open() throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(sheetsParameters.getServiceAccountKey());
        ServiceAccountCredentials sourceCredentials = ServiceAccountCredentials
                .fromStream(new ByteArrayInputStream(keyBytes));
        sourceCredentials = (ServiceAccountCredentials) sourceCredentials
                .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS));
        requestInitializer = new HttpCredentialsAdapter(sourceCredentials);

    }

    @Override
    public void close() throws IOException {
        // no need close
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        Sheets service = new Sheets.Builder(httpTransport, JSON_FACTORY, requestInitializer)
                .setApplicationName(APPLICATION_NAME)
                .build();
        ValueRange response = service.spreadsheets().values()
                .get(sheetsParameters.getSheetId(), sheetsParameters.getSheetName() + "!" + sheetsParameters.getRange())
                .execute();
        List<List<Object>> values = response.getValues();
        if (values != null) {
            for (List<Object> row : values) {
                SeaTunnelRow seaTunnelRow = this.seaTunnelRowDeserializer.deserializeRow(row);
                output.collect(seaTunnelRow);
            }
        }
        this.context.signalNoMoreElement();
    }
}
