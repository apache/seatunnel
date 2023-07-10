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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;

public class BsonUtils {

    public static int compareBsonValue(BsonValue o1, BsonValue o2) {
        return compareBsonValue(o1, o2, true);
    }

    private static int compareBsonValue(BsonValue o1, BsonValue o2, boolean isTopLevel) {
        if (Objects.equals(o1, o2)) {
            return 0;
        }

        if (isTopLevel) {
            BsonValue element1 = o1;
            BsonValue element2 = o2;

            if (o1 != null && o1.isArray()) {
                element1 = smallestValueOfArray(o1.asArray());
            }
            if (o2.isArray()) {
                element2 = smallestValueOfArray(o2.asArray());
            }
            return compareBsonValues(element1, element2);
        }
        if (typeOrder(o1) != typeOrder(o2)) {
            return Integer.compare(typeOrder(o1), typeOrder(o2));
        }

        if (isNull(o1) || isMinKey(o1) || isMaxKey(o1)) {
            return 0; // Null == Null, MinKey == MinKey, MaxKey == MaxKey
        }

        switch (o1.getBsonType()) {
            case INT32:
            case INT64:
            case DOUBLE:
                return compareBsonNumbers(o1.asNumber(), o2.asNumber());
            case STRING:
            case JAVASCRIPT:
            case REGULAR_EXPRESSION:
                return compareStrings(o1.asString().getValue(), o2.asString().getValue());
            case BOOLEAN:
                return compareBooleans(o1.asBoolean().getValue(), o2.asBoolean().getValue());
            case DATE_TIME:
                return compareDateTimes(o1.asDateTime().getValue(), o2.asDateTime().getValue());
            case TIMESTAMP:
                return compareTimestamps(o1.asTimestamp().getValue(), o2.asTimestamp().getValue());
            case BINARY:
                return compareBsonBinary(o1.asBinary(), o2.asBinary());
            case OBJECT_ID:
                return o1.asObjectId().compareTo(o2.asObjectId());
            case DOCUMENT:
            case DB_POINTER:
                return compareBsonDocument(toBsonDocument(o1), toBsonDocument(o2));
            case ARRAY:
                return compareBsonArray(o1.asArray(), o2.asArray());
            case JAVASCRIPT_WITH_SCOPE:
                return compareJavascriptWithScope(
                        o1.asJavaScriptWithScope(), o2.asJavaScriptWithScope());
            default:
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        String.format("Unable to compare bson values between %s and %s", o1, o2));
        }
    }

    private static int compareBsonValues(BsonValue v1, BsonValue v2) {
        return compareBsonValue(v1, v2, false);
    }

    private static int compareBsonNumbers(BsonNumber n1, BsonNumber n2) {
        Decimal128 decimal1 = getDecimal128FromCache(n1);
        Decimal128 decimal2 = getDecimal128FromCache(n2);
        return decimal1.compareTo(decimal2);
    }

    private static int compareStrings(String s1, String s2) {
        return getStringFromCache(s1).compareTo(getStringFromCache(s2));
    }

    private static int compareBooleans(boolean b1, boolean b2) {
        return Boolean.compare(b1, b2);
    }

    private static int compareDateTimes(long dt1, long dt2) {
        return Long.compare(dt1, dt2);
    }

    private static int compareTimestamps(long ts1, long ts2) {
        return Long.compare(ts1, ts2);
    }

    private static final Map<BsonValue, Decimal128> decimalCache = new HashMap<>();
    private static final Map<String, String> stringCache = new HashMap<>();

    private static Decimal128 getDecimal128FromCache(BsonValue value) {
        return decimalCache.computeIfAbsent(value, BsonUtils::toDecimal128);
    }

    private static String getStringFromCache(String value) {
        return stringCache.computeIfAbsent(value, k -> k);
    }

    public static int compareBsonDocument(@Nonnull BsonDocument d1, @Nonnull BsonDocument d2) {
        Iterator<Map.Entry<String, BsonValue>> iterator1 = d1.entrySet().iterator();
        Iterator<Map.Entry<String, BsonValue>> iterator2 = d2.entrySet().iterator();

        if (!iterator1.hasNext() && !iterator2.hasNext()) {
            return 0;
        } else if (!iterator1.hasNext()) {
            return -1;
        } else if (!iterator2.hasNext()) {
            return 1;
        } else {
            while (iterator1.hasNext() && iterator2.hasNext()) {
                Map.Entry<String, BsonValue> entry1 = iterator1.next();
                Map.Entry<String, BsonValue> entry2 = iterator2.next();

                int result =
                        Integer.compare(typeOrder(entry1.getValue()), typeOrder(entry2.getValue()));
                if (result != 0) {
                    return result;
                }

                result = entry1.getKey().compareTo(entry2.getKey());
                if (result != 0) {
                    return result;
                }

                result = compareBsonValue(entry1.getValue(), entry2.getValue(), false);
                if (result != 0) {
                    return result;
                }
            }

            return Integer.compare(d1.size(), d2.size());
        }
    }

    public static int compareBsonArray(BsonArray a1, BsonArray a2) {
        return compareBsonValue(smallestValueOfArray(a1), smallestValueOfArray(a2), false);
    }

    private static BsonValue smallestValueOfArray(@Nonnull BsonArray bsonArray) {
        if (bsonArray.isEmpty()) {
            return new BsonUndefined();
        }

        if (bsonArray.size() == 1) {
            return bsonArray.get(0);
        }

        return bsonArray.getValues().stream()
                .min((e1, e2) -> compareBsonValue(e1, e2, false))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Unable to find smallest value in the array."));
    }

    public static int compareBsonBinary(@Nonnull BsonBinary b1, @Nonnull BsonBinary b2) {
        byte[] data1 = b1.getData();
        byte[] data2 = b2.getData();

        int lengthComparison = Integer.compare(data1.length, data2.length);
        if (lengthComparison != 0) {
            return lengthComparison;
        }

        int typeComparison = Byte.compare(b1.getType(), b2.getType());
        if (typeComparison != 0) {
            return typeComparison;
        }

        for (int i = 0; i < data1.length; i++) {
            int byteComparison = Integer.compareUnsigned(data1[i] & 0xff, data2[i] & 0xff);
            if (byteComparison != 0) {
                return byteComparison;
            }
        }

        return 0;
    }

    public static int compareJavascriptWithScope(
            @Nonnull BsonJavaScriptWithScope c1, @Nonnull BsonJavaScriptWithScope c2) {
        int result = c1.getCode().compareTo(c2.getCode());
        if (result != 0) {
            return result;
        }
        return compareBsonDocument(c1.getScope(), c2.getScope());
    }

    public static boolean isNull(BsonValue bsonValue) {
        return bsonValue == null
                || bsonValue.isNull()
                || bsonValue.getBsonType() == BsonType.UNDEFINED;
    }

    public static boolean isMinKey(BsonValue bsonValue) {
        return bsonValue != null && bsonValue.getBsonType() == BsonType.MIN_KEY;
    }

    public static boolean isMaxKey(BsonValue bsonValue) {
        return bsonValue != null && bsonValue.getBsonType() == BsonType.MAX_KEY;
    }

    public static Decimal128 toDecimal128(@Nonnull BsonValue bsonValue) {
        if (bsonValue.isNumber()) {
            return bsonValue.asNumber().decimal128Value();
        } else if (bsonValue.isDecimal128()) {
            return bsonValue.asDecimal128().decimal128Value();
        } else {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "Cannot convert to Decimal128 with unexpected value: " + bsonValue);
        }
    }

    public static BsonDocument toBsonDocument(@Nonnull BsonValue bsonValue) {
        if (bsonValue.isDocument()) {
            return bsonValue.asDocument();
        } else if (bsonValue.isDBPointer()) {
            BsonDbPointer dbPointer = bsonValue.asDBPointer();
            return new BsonDocument("$ref", new BsonString(dbPointer.getNamespace()))
                    .append("$id", new BsonObjectId(dbPointer.getId()));
        }

        throw new MongodbConnectorException(
                ILLEGAL_ARGUMENT, "Cannot convert to Document with unexpected value: " + bsonValue);
    }

    public static int typeOrder(BsonValue bsonValue) {
        // Missing Key field
        if (bsonValue == null) {
            return 3;
        }

        BsonType bsonType = bsonValue.getBsonType();
        switch (bsonType) {
            case MIN_KEY:
                return 1;
            case UNDEFINED:
                return 2;
            case NULL:
                return 3;
            case INT32:
            case INT64:
            case DOUBLE:
            case DECIMAL128:
                return 4;
            case STRING:
            case SYMBOL:
                return 5;
            case DOCUMENT:
            case DB_POINTER:
                return 6;
            case ARRAY:
                return 7;
            case BINARY:
                return 8;
            case OBJECT_ID:
                return 9;
            case BOOLEAN:
                return 10;
            case DATE_TIME:
                return 11;
            case TIMESTAMP:
                return 12;
            case REGULAR_EXPRESSION:
                return 13;
            case JAVASCRIPT:
                return 14;
            case JAVASCRIPT_WITH_SCOPE:
                return 15;
            case MAX_KEY:
                return 99;
            default:
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT, "Unknown bson type : " + bsonType);
        }
    }
}
