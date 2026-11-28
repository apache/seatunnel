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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

/**
 * Registers the isolated AmazonDataFirehose sink and its dedicated option contract through SPI.
 * Resource validation and row-format ownership remain in the product-specific sink.
 */
@AutoService(Factory.class)
public class AmazonDataFirehoseSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "AmazonDataFirehose";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AmazonDataFirehoseSinkOptions.DELIVERY_STREAM,
                        AmazonDataFirehoseSinkOptions.REGION)
                .optional(
                        AmazonDataFirehoseSinkOptions.ACCESS_KEY,
                        AmazonDataFirehoseSinkOptions.SECRET_KEY,
                        AmazonDataFirehoseSinkOptions.TOKEN,
                        AmazonDataFirehoseSinkOptions.ENDPOINT_URL,
                        AmazonDataFirehoseSinkOptions.ALLOW_INSECURE_HTTP,
                        AmazonDataFirehoseSinkOptions.FORMAT,
                        AmazonDataFirehoseSinkOptions.FIELD_DELIMITER,
                        AmazonDataFirehoseSinkOptions.MAX_RECORD_BYTES,
                        AmazonDataFirehoseSinkOptions.BATCH_SIZE,
                        AmazonDataFirehoseSinkOptions.BATCH_TIMEOUT_MS,
                        AmazonDataFirehoseSinkOptions.MAX_RETRIES)
                .build();
    }

    /*
     * Defer worker writer/client construction while retaining the factory-provided catalog and options.
     */
    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new AmazonDataFirehoseSink(context.getOptions(), context.getCatalogTable());
    }
}
