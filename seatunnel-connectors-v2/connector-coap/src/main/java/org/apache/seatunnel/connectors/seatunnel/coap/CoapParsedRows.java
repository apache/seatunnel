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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;

/** Applies an optional SeaTunnel column schema to the structured representation. */
final class CoapParsedRows {
    private final JsonDeserializationSchema converter;

    CoapParsedRows(CoapSettings.Listener settings, CatalogTable table) {
        converter = settings.typed ? new JsonDeserializationSchema(table, false, false) : null;
    }

    SeaTunnelRow row(JsonNode data) throws IOException {
        if (converter != null && !data.isObject()) {
            throw new IOException("typed CoAP payload requires a record object");
        }
        return converter == null
                ? new SeaTunnelRow(new Object[] {data.toString()})
                : converter.deserialize(data.toString());
    }
}
