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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.utils;

import io.debezium.DebeziumException;
import io.debezium.util.HexConverter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Utils for serialization and deserialization. */
public class SerializerUtils {

    private SerializerUtils() {}

    public static String rowToSerializedString(Object[] splitBoundary) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(splitBoundary);
            return HexConverter.convertToHexString(bos.toByteArray());
        } catch (IOException e) {
            throw new DebeziumException(
                    String.format("Cannot serialize split boundary information %s", splitBoundary));
        }
    }

    public static String rowToSerializedString(Object splitBoundary) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(splitBoundary);
            return HexConverter.convertToHexString(bos.toByteArray());
        } catch (IOException e) {
            throw new DebeziumException(
                    String.format("Cannot serialize split boundary information %s", splitBoundary));
        }
    }

    public static Object[] serializedStringToRow(String serialized) {
        try (final ByteArrayInputStream bis =
                        new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Object[]) ois.readObject();
        } catch (Exception e) {
            throw new DebeziumException(
                    String.format(
                            "Failed to deserialize split boundary with value '%s'", serialized),
                    e);
        }
    }

    public static Object serializedStringToObject(String serialized) {
        try (final ByteArrayInputStream bis =
                        new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (Exception e) {
            throw new DebeziumException(
                    String.format(
                            "Failed to deserialize split boundary with value '%s'", serialized),
                    e);
        }
    }
}
