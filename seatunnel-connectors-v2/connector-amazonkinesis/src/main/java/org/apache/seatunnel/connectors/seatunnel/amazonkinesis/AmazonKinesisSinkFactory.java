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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public final class AmazonKinesisSinkFactory
        implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {
    @Override
    public String factoryIdentifier() {
        return AmazonKinesisSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KinesisConnection.Settings.REGION)
                .optional(
                        KinesisConnection.Settings.STREAM_NAME,
                        KinesisConnection.Settings.STREAM_ARN,
                        KinesisConnection.Settings.ACCESS_KEY,
                        KinesisConnection.Settings.SECRET_KEY,
                        KinesisConnection.Settings.TOKEN,
                        KinesisConnection.Settings.API_TIMEOUT_MS,
                        AmazonKinesisSinkOptions.FORMAT,
                        AmazonKinesisSinkOptions.DATA_FIELD,
                        AmazonKinesisSinkOptions.PARTITION_KEY,
                        AmazonKinesisSinkOptions.PARTITION_KEY_FIELD,
                        AmazonKinesisSinkOptions.BATCH_SIZE,
                        AmazonKinesisSinkOptions.BATCH_BYTES,
                        AmazonKinesisSinkOptions.RECORD_BYTES,
                        AmazonKinesisSinkOptions.PARTIAL_ATTEMPTS,
                        AmazonKinesisSinkOptions.PRESERVE_ORDER)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableSinkFactoryContext context) {
        return () -> new AmazonKinesisSink(context.getOptions(), context.getCatalogTable());
    }
}
