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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.LossyCoercion;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Objects;

public class Neo4jSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final SingleSplitReaderContext context;
    private final Neo4jSourceQueryInfo neo4jSourceQueryInfo;
    private final SeaTunnelRowType rowType;
    private final Driver driver;
    private Session session;

    public Neo4jSourceReader(SingleSplitReaderContext context, Neo4jSourceQueryInfo neo4jSourceQueryInfo,
                             SeaTunnelRowType rowType) {
        this.context = context;
        this.neo4jSourceQueryInfo = neo4jSourceQueryInfo;
        this.driver = neo4jSourceQueryInfo.getDriverBuilder().build();
        this.rowType = rowType;
    }

    @Override
    public void open() throws Exception {
        this.session = driver.session(SessionConfig.forDatabase(neo4jSourceQueryInfo.getDriverBuilder().getDatabase()));
    }

    @Override
    public void close() throws IOException {
        session.close();
        driver.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        final Query query = new Query(neo4jSourceQueryInfo.getQuery());
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

    /**
     * convert {@link SeaTunnelDataType} to java data type
     *
     * @throws Neo4jConnectorException when not supported data type
     * @throws LossyCoercion           when conversion cannot be achieved without losing precision.
     */
    public static Object convertType(SeaTunnelDataType<?> dataType, Value value)
        throws Neo4jConnectorException, LossyCoercion {
        Objects.requireNonNull(dataType);
        Objects.requireNonNull(value);

        switch (dataType.getSqlType()) {
            case STRING:
                return value.asString();
            case BOOLEAN:
                return value.asBoolean();
            case BIGINT:
                return value.asLong();
            case DOUBLE:
                return value.asDouble();
            case NULL:
                return null;
            case BYTES:
                return value.asByteArray();
            case DATE:
                return value.asLocalDate();
            case TIME:
                return value.asLocalTime();
            case TIMESTAMP:
                return value.asLocalDateTime();
            case MAP:
                if (!((MapType<?, ?>) dataType).getKeyType().equals(BasicType.STRING_TYPE)) {
                    throw new Neo4jConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, "Key Type of MapType must String type");
                }
                final SeaTunnelDataType<?> valueType = ((MapType<?, ?>) dataType).getValueType();
                return value.asMap(v -> valueType.getTypeClass().cast(convertType(valueType, v)));
            case ARRAY:
                final BasicType<?> elementType = ((ArrayType<?, ?>) dataType).getElementType();
                final List<?> list = value.asList(v -> elementType.getTypeClass().cast(convertType(elementType, v)));
                final Object array = Array.newInstance(elementType.getTypeClass(), list.size());
                for (int i = 0; i < list.size(); i++) {
                    Array.set(array, i, list.get(i));
                }
                return array;
            case INT:
                return value.asInt();
            case FLOAT:
                return value.asFloat();
            default:
                throw new Neo4jConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "not supported data type: " + dataType);
        }

    }
}
