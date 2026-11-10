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

package org.apache.seatunnel.connectors.seatunnel.woocommerce.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

final class WooCommerceSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final WooCommerceConfig config;
    private final SingleSplitReaderContext context;
    private final JsonDeserializationSchema deserializer;
    private final SeaTunnelRowType rowType;
    private volatile boolean closed;
    private WooCommerceClient client;

    WooCommerceSourceReader(
            WooCommerceConfig config, CatalogTable table, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
        deserializer = new JsonDeserializationSchema(table, false, false);
        rowType = table.getSeaTunnelRowType();
    }

    WooCommerceSourceReader(
            WooCommerceConfig config,
            CatalogTable table,
            SingleSplitReaderContext context,
            WooCommerceClient client) {
        this(config, table, context);
        this.client = client;
    }

    /** Create transport on the worker, never in the factory or split state. */
    @Override
    public synchronized void open() {
        if (closed || client != null) {
            throw WooCommerceClient.failure("Reader cannot be opened");
        }
        client = new WooCommerceClient(config);
    }

    /** Restart the bounded scan on recovery; remote page offsets do not identify a snapshot. */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        long total = -1;
        int pages = -1;
        long previousId = 0;
        for (int page = 1; ; page++) {
            checkOpen();
            WooCommerceClient.Page response = client.page(page);
            if (total == -1) {
                total = response.total;
                pages = response.pages;
            } else if (total != response.total || pages != response.pages) {
                throw WooCommerceClient.failure(
                        "Order totals changed during pagination; retry a quiescent time window");
            }
            for (JsonNode order : response.rows) {
                checkOpen();
                long id = order.get("id").longValue();
                if (id <= previousId) {
                    throw WooCommerceClient.failure(
                            "Order IDs are not strictly ascending; unstable pagination");
                }
                previousId = id;
                SeaTunnelRow row;
                try {
                    row = deserializer.deserialize(order.toString());
                    validateDecimals(row, rowType);
                } catch (Exception e) {
                    throw WooCommerceClient.failure(
                            "Order does not match configured schema (record withheld)");
                }
                output.collect(row);
            }
            if (page >= pages) {
                break;
            }
        }
        checkOpen();
        context.signalNoMoreElement();
    }

    private void checkOpen() {
        if (closed || Thread.currentThread().isInterrupted() || client == null) {
            throw WooCommerceClient.failure("Reader is not open or was cancelled");
        }
    }

    private static void validateDecimals(Object value, SeaTunnelDataType<?> type) {
        if (value == null) {
            return;
        }
        switch (type.getSqlType()) {
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                BigDecimal amount = ((BigDecimal) value).stripTrailingZeros();
                if (amount.scale() > decimal.getScale()) {
                    throw WooCommerceClient.failure("DECIMAL exceeds schema scale");
                }
                if (amount.signum() != 0
                        && (long) amount.precision() - amount.scale()
                                > (long) decimal.getPrecision() - decimal.getScale()) {
                    throw WooCommerceClient.failure("DECIMAL exceeds schema precision");
                }
                BigDecimal exact = amount.setScale(decimal.getScale(), RoundingMode.UNNECESSARY);
                if (exact.precision() > decimal.getPrecision()) {
                    throw WooCommerceClient.failure("DECIMAL exceeds schema precision");
                }
                break;
            case ROW:
                SeaTunnelRowType row = (SeaTunnelRowType) type;
                for (int i = 0; i < row.getTotalFields(); i++) {
                    validateDecimals(((SeaTunnelRow) value).getField(i), row.getFieldType(i));
                }
                break;
            case ARRAY:
                for (Object element : (Object[]) value) {
                    validateDecimals(element, ((ArrayType<?, ?>) type).getElementType());
                }
                break;
            case MAP:
                MapType<?, ?> map = (MapType<?, ?>) type;
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    validateDecimals(entry.getKey(), map.getKeyType());
                    validateDecimals(entry.getValue(), map.getValueType());
                }
                break;
            default:
                break;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (client != null) {
            client.close();
        }
    }
}
