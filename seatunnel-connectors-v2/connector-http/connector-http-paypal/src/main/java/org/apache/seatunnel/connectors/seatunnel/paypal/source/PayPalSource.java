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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.util.Collections;
import java.util.List;

/** A single-account bounded report, preserving reporting records without snapshot guarantees. */
public class PayPalSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    public static final String PLUGIN_NAME = "PayPal";
    private final PayPalConfig config;

    public PayPalSource(ReadonlyConfig options) {
        config = new PayPalConfig(options);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw new IllegalArgumentException("PayPal source supports BATCH only");
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema.Builder schema = TableSchema.builder();
        String[] names = {
            "account_number",
            "transaction_id",
            "transaction_event_code",
            "transaction_status",
            "transaction_initiation_date",
            "transaction_updated_date",
            "transaction_amount",
            "transaction_currency",
            "fee_amount",
            "fee_currency",
            "content"
        };
        for (int i = 0; i < names.length; i++) {
            SeaTunnelDataType<?> type =
                    i == 6 || i == 8 ? new DecimalType(38, 9) : BasicType.STRING_TYPE;
            schema.column(PhysicalColumn.of(names[i], type, 0, i != 0 && i != 10, null, null));
        }
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(PLUGIN_NAME, TablePath.DEFAULT),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) {
        return new PayPalSourceReader(config, context);
    }
}
