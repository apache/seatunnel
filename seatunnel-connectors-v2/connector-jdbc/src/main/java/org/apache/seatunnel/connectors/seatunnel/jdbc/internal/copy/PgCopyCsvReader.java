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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public final class PgCopyCsvReader implements PgCopyReader {

    private static final Logger LOG = LoggerFactory.getLogger(PgCopyCsvReader.class);

    private final Iterator<CSVRecord> iterator;
    private final TableSchema schema;
    private final CSVParser parser;
    private boolean hasNext;

    public PgCopyCsvReader(InputStream inputStream, TableSchema schema) throws IOException {
        this.schema = schema;
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        this.parser = new CSVParser(reader, CSVFormat.POSTGRESQL_CSV);
        this.iterator = parser.iterator();
        this.hasNext = iterator.hasNext();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public SeaTunnelRow next() {
        if (!iterator.hasNext()) {
            hasNext = false;
            return null;
        }

        CSVRecord record = iterator.next();
        hasNext = iterator.hasNext();
        return parseCsvRow(record, schema);
    }

    private SeaTunnelRow parseCsvRow(CSVRecord record, TableSchema schema) {
        SeaTunnelRowType rowType = schema.toPhysicalRowDataType();
        Object[] values = new Object[rowType.getTotalFields()];

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String raw = record.get(i);
            SeaTunnelDataType<?> type = rowType.getFieldType(i);
            values[i] = PgCopyUtils.parseValue(raw, type);
        }

        return new SeaTunnelRow(values);
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }
}
