/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.seatunnel.connectors.seatunnel.mem0.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.mem0.config.Mem0Options;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class Mem0SinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Mem0";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(Mem0Options.API_KEY, Mem0Options.MESSAGES_FIELD)
                .optional(
                        Mem0Options.API_BASE_URL,
                        Mem0Options.AGENT_ID_FIELD,
                        Mem0Options.APP_ID_FIELD,
                        Mem0Options.RUN_ID_FIELD,
                        Mem0Options.METADATA_FIELD,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new Mem0Sink(context.getOptions(), context.getCatalogTable());
    }
}
