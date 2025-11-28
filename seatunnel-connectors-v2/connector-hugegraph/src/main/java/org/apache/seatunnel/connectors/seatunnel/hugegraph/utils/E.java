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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import javax.annotation.Nullable;

import java.util.Collection;

public final class E {

    public static void checkNotNull(Object object, String elem) {
        if (object == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("The '%s' can't be null", elem));
        }
    }

    public static void checkNotNull(Object object, String elem, String owner) {
        if (object == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("The '%s' of '%s' can't be null", elem, owner));
        }
    }

    public static void checkNotEmpty(Collection<?> collection, String elem) {
        if (collection == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("The '%s' can't be null", elem));
        }
        if (collection.isEmpty()) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("The '%s' can't be empty", elem));
        }
    }

    public static void checkNotEmpty(Collection<?> collection, String elem, String owner) {
        if (collection == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("The '%s' of '%s' can't be null", elem, owner));
        }
        if (collection.isEmpty()) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("The '%s' of '%s' can't be empty", elem, owner));
        }
    }

    public static void checkArgument(
            boolean expression, @Nullable String message, @Nullable Object... args) {
        if (!expression) {
            String formattedMessage =
                    (message == null || args == null || args.length == 0)
                            ? (message != null ? message : "")
                            : String.format(message, args);
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT, formattedMessage);
        }
    }

    public static void checkArgumentNotNull(
            Object object, @Nullable String message, @Nullable Object... args) {
        checkArgument(object != null, message, args);
    }

    public static void checkState(
            boolean expression, @Nullable String message, @Nullable Object... args) {
        if (!expression) {
            String formattedMessage =
                    (message == null || args == null || args.length == 0)
                            ? (message != null ? message : "")
                            : String.format(message, args);
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT, formattedMessage);
        }
    }
}
