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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceConfig;
import org.neo4j.driver.*;

import java.io.IOException;
import java.util.Objects;

public class Neo4jSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final SingleSplitReaderContext context;
    private final Neo4jSourceConfig config;
    private final SeaTunnelRowType rowType;
    private final Driver driver;
    private Session session;


    public Neo4jSourceReader(SingleSplitReaderContext context, Neo4jSourceConfig config, SeaTunnelRowType rowType) {
        this.context = context;
        this.config = config;
        this.driver = config.getDriverBuilder().build();
        this.rowType = rowType;
    }

    @Override
    public void open() throws Exception {
        this.session = driver.session(SessionConfig.forDatabase(config.getDriverBuilder().getDatabase()));
    }

    @Override
    public void close() throws IOException {
        session.close();
        driver.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        final Query query = new Query(config.getQuery());
        session.readTransaction(tx -> {
            final Result result = tx.run(query);
            result.stream()
                    .forEach(row -> {
                        final Object[] fields = new Object[rowType.getTotalFields()];
                        for (int i = 0; i < rowType.getTotalFields(); i++) {
                            final String fieldName = rowType.getFieldName(i);
                            final SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
                            final Value value = row.get(fieldName);
                            fields[i] = convertType(fieldType, value);
                        }
                        output.collect(new SeaTunnelRow(fields));
                    });
            return null;
        });
        this.context.signalNoMoreElement();
    }

    public static Object convertType(SeaTunnelDataType<?> dataType, Value value) {
        Objects.requireNonNull(dataType);
        Objects.requireNonNull(value);

        if (dataType.equals(BasicType.STRING_TYPE)) {
            return value.asString();
        } else if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return value.asBoolean();
        } else if (dataType.equals(BasicType.LONG_TYPE)) {
            return value.asLong();
        } else if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return value.asDouble();
        } else if (dataType.equals(BasicType.VOID_TYPE)) {
            return null;
        } else if (dataType.equals(PrimitiveByteArrayType.INSTANCE)) {
            return value.asByteArray();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TYPE)) {
            return value.asLocalDate();
        } else if (dataType.equals(LocalTimeType.LOCAL_TIME_TYPE)) {
            return value.asLocalTime();
        } else if (dataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            return value.asLocalDateTime();
        } else if (dataType instanceof MapType) {
            return value.asMap();
        } else if (dataType.equals(BasicType.INT_TYPE)) {
            return value.asInt();
        } else if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return value.asFloat();
        } else {
            throw new IllegalArgumentException("not supported data type: " + dataType);
        }
    }
}
