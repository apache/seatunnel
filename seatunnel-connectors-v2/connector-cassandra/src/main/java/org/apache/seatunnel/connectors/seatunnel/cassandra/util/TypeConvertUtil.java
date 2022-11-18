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

package org.apache.seatunnel.connectors.seatunnel.cassandra.util;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class TypeConvertUtil {
    public static SeaTunnelDataType<?> convert(DataType type) {
        switch (type.getProtocolCode()) {
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.VARINT:
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.TIMEUUID:
                return BasicType.STRING_TYPE;
            case ProtocolConstants.DataType.TINYINT:
                return BasicType.BYTE_TYPE;
            case ProtocolConstants.DataType.SMALLINT:
                return BasicType.SHORT_TYPE;
            case ProtocolConstants.DataType.INT:
                return BasicType.INT_TYPE;
            case ProtocolConstants.DataType.BIGINT:
            case ProtocolConstants.DataType.COUNTER:
                return BasicType.LONG_TYPE;
            case ProtocolConstants.DataType.FLOAT:
                return BasicType.FLOAT_TYPE;
            case ProtocolConstants.DataType.DOUBLE:
            case ProtocolConstants.DataType.DECIMAL:
                return BasicType.DOUBLE_TYPE;
            case ProtocolConstants.DataType.BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case ProtocolConstants.DataType.TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case ProtocolConstants.DataType.DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case ProtocolConstants.DataType.TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case ProtocolConstants.DataType.BLOB:
                return ArrayType.BYTE_ARRAY_TYPE;
            case ProtocolConstants.DataType.MAP:
                return new MapType<>(convert(((DefaultMapType) type).getKeyType()), convert(((DefaultMapType) type).getValueType()));
            case ProtocolConstants.DataType.LIST:
                return convertToArrayType(convert(((DefaultListType) type).getElementType()));
            case ProtocolConstants.DataType.SET:
                return convertToArrayType(convert(((DefaultSetType) type).getElementType()));
            default:
                throw new CassandraConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported this data type: " + type);
        }
    }

    private static ArrayType<?, ?> convertToArrayType(SeaTunnelDataType<?> dataType) {
        if (dataType.equals(BasicType.STRING_TYPE)) {
            return ArrayType.STRING_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.BYTE_TYPE)) {
            return ArrayType.BYTE_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.SHORT_TYPE)) {
            return ArrayType.SHORT_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.INT_TYPE)) {
            return ArrayType.INT_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.LONG_TYPE)) {
            return ArrayType.LONG_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return ArrayType.FLOAT_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return ArrayType.DOUBLE_ARRAY_TYPE;
        } else if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return ArrayType.BOOLEAN_ARRAY_TYPE;
        } else {
            throw new CassandraConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported this data type: " + dataType);
        }
    }

    public static SeaTunnelRow buildSeaTunnelRow(Row row) {
        DataType subType;
        Class<?> typeClass;
        Object[] fields = new Object[row.size()];
        ColumnDefinitions metaData = row.getColumnDefinitions();
        for (int i = 0; i < row.size(); i++) {
            switch (metaData.get(i).getType().getProtocolCode()) {
                case ProtocolConstants.DataType.ASCII:
                case ProtocolConstants.DataType.VARCHAR:
                    fields[i] = row.getString(i);
                    break;
                case ProtocolConstants.DataType.VARINT:
                    fields[i] = Objects.requireNonNull(row.getBigInteger(i)).toString();
                    break;
                case ProtocolConstants.DataType.TIMEUUID:
                case ProtocolConstants.DataType.UUID:
                    fields[i] = Objects.requireNonNull(row.getUuid(i)).toString();
                    break;
                case ProtocolConstants.DataType.INET:
                    fields[i] = Objects.requireNonNull(row.getInetAddress(i)).getHostAddress();
                    break;
                case ProtocolConstants.DataType.TINYINT:
                    fields[i] = row.getByte(i);
                    break;
                case ProtocolConstants.DataType.SMALLINT:
                    fields[i] = row.getShort(i);
                    break;
                case ProtocolConstants.DataType.INT:
                    fields[i] = row.getInt(i);
                    break;
                case ProtocolConstants.DataType.BIGINT:
                    fields[i] = row.getLong(i);
                    break;
                case ProtocolConstants.DataType.FLOAT:
                    fields[i] = row.getFloat(i);
                    break;
                case ProtocolConstants.DataType.DOUBLE:
                    fields[i] = row.getDouble(i);
                    break;
                case ProtocolConstants.DataType.DECIMAL:
                    fields[i] = Objects.requireNonNull(row.getBigDecimal(i)).doubleValue();
                    break;
                case ProtocolConstants.DataType.BOOLEAN:
                    fields[i] = row.getBoolean(i);
                    break;
                case ProtocolConstants.DataType.TIME:
                    fields[i] = row.getLocalTime(i);
                    break;
                case ProtocolConstants.DataType.DATE:
                    fields[i] = row.getLocalDate(i);
                    break;
                case ProtocolConstants.DataType.TIMESTAMP:
                    fields[i] = Timestamp.from(Objects.requireNonNull(row.getInstant(i))).toLocalDateTime();
                    break;
                case ProtocolConstants.DataType.BLOB:
                    fields[i] = ArrayUtils.toObject(Objects.requireNonNull(row.getByteBuffer(i)).array());
                    break;
                case ProtocolConstants.DataType.MAP:
                    subType = metaData.get(i).getType();
                    fields[i] = row.getMap(i, convert(((DefaultMapType) subType).getKeyType()).getTypeClass(), convert(((DefaultMapType) subType).getValueType()).getTypeClass());
                    break;
                case ProtocolConstants.DataType.LIST:
                    typeClass = convert(((DefaultListType) metaData.get(i).getType()).getElementType()).getTypeClass();
                    if (String.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, String.class)).toArray(new String[0]);
                    } else if (Byte.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Byte.class)).toArray(new Byte[0]);
                    } else if (Short.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Short.class)).toArray(new Short[0]);
                    } else if (Integer.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Integer.class)).toArray(new Integer[0]);
                    } else if (Long.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Long.class)).toArray(new Long[0]);
                    } else if (Float.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Float.class)).toArray(new Float[0]);
                    } else if (Double.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Double.class)).toArray(new Double[0]);
                    } else if (Boolean.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getList(i, Boolean.class)).toArray(new Boolean[0]);
                    } else {
                        throw new CassandraConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "List unsupported this data type: " + typeClass.toString());
                    }
                    break;
                case ProtocolConstants.DataType.SET:
                    typeClass = convert(((DefaultSetType) metaData.get(i).getType()).getElementType()).getTypeClass();
                    if (String.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, String.class)).toArray(new String[0]);
                    } else if (Byte.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Byte.class)).toArray(new Byte[0]);
                    } else if (Short.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Short.class)).toArray(new Short[0]);
                    } else if (Integer.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Integer.class)).toArray(new Integer[0]);
                    } else if (Long.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Long.class)).toArray(new Long[0]);
                    } else if (Float.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Float.class)).toArray(new Float[0]);
                    } else if (Double.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Double.class)).toArray(new Double[0]);
                    } else if (Boolean.class.equals(typeClass)) {
                        fields[i] = Objects.requireNonNull(row.getSet(i, Boolean.class)).toArray(new Boolean[0]);
                    } else {
                        throw new CassandraConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "List unsupported this data type: " + typeClass.toString());
                    }
                    break;
                default:
                    fields[i] = row.getObject(i);
            }
        }
        return new SeaTunnelRow(fields);
    }

    public static BoundStatement reconvertAndInject(BoundStatement statement, int index, DataType type, Object fileValue) {
        switch (type.getProtocolCode()) {
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.ASCII:
                statement = statement.setString(index, (String) fileValue);
                return statement;
            case ProtocolConstants.DataType.VARINT:
                statement = statement.setBigInteger(index, new BigInteger((String) fileValue));
                return statement;
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
                statement = statement.setUuid(index, UUID.fromString((String) fileValue));
                return statement;
            case ProtocolConstants.DataType.INET:
                try {
                    statement = statement.setInetAddress(index, InetAddress.getByName((String) fileValue));
                } catch (UnknownHostException e) {
                    throw new CassandraConnectorException(CassandraConnectorErrorCode.PARSE_IP_ADDRESS_FAILED, e);
                }
                return statement;
            case ProtocolConstants.DataType.TINYINT:
                statement = statement.setByte(index, (Byte) fileValue);
                return statement;
            case ProtocolConstants.DataType.SMALLINT:
                statement = statement.setShort(index, (Short) fileValue);
                return statement;
            case ProtocolConstants.DataType.INT:
                statement = statement.setInt(index, (Integer) fileValue);
                return statement;
            case ProtocolConstants.DataType.BIGINT:
            case ProtocolConstants.DataType.COUNTER:
                statement = statement.setLong(index, (Long) fileValue);
                return statement;
            case ProtocolConstants.DataType.FLOAT:
                statement = statement.setFloat(index, (Float) fileValue);
                return statement;
            case ProtocolConstants.DataType.DOUBLE:
                statement = statement.setDouble(index, (Double) fileValue);
                return statement;
            case ProtocolConstants.DataType.DECIMAL:
                statement = statement.setBigDecimal(index, BigDecimal.valueOf((Double) fileValue));
                return statement;
            case ProtocolConstants.DataType.BOOLEAN:
                statement = statement.setBoolean(index, (Boolean) fileValue);
                return statement;
            case ProtocolConstants.DataType.TIME:
                statement = statement.setLocalTime(index, (LocalTime) fileValue);
                return statement;
            case ProtocolConstants.DataType.DATE:
                statement = statement.setLocalDate(index, (LocalDate) fileValue);
                return statement;
            case ProtocolConstants.DataType.TIMESTAMP:
                statement = statement.setInstant(index, ((LocalDateTime) fileValue).atZone(ZoneId.systemDefault()).toInstant());
                return statement;
            case ProtocolConstants.DataType.BLOB:
                if (fileValue.getClass().equals(Object[].class)) {
                    fileValue = Arrays.stream((Object[]) fileValue).toArray(Byte[]::new);
                }
                statement = statement.setByteBuffer(index, ByteBuffer.wrap(ArrayUtils.toPrimitive((Byte[]) fileValue)));
                return statement;
            case ProtocolConstants.DataType.MAP:
                statement = statement.set(index, (Map<?, ?>) fileValue, Map.class);
                return statement;
            case ProtocolConstants.DataType.LIST:
                statement = statement.set(index, Arrays.stream((Object[]) fileValue).collect(Collectors.toList()), List.class);
                return statement;
            case ProtocolConstants.DataType.SET:
                statement = statement.set(index, Arrays.stream((Object[]) fileValue).collect(Collectors.toSet()), Set.class);
                return statement;
            default:
                statement = statement.set(index, fileValue, Object.class);
                return statement;
        }
    }

}
