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

package io.debezium.connector.oracle.logminer.parser;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Utility helper methods for the Oracle LogMiner XML parsing classes.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/XmlParserUtils.java}. Binary XML rows have {@code INFO}
 * ending with {@code not re-executable}.
 *
 * <p>SeaTunnel changes: {@code [SeaTunnel]} {@link #isXmlSerializedAsBinary(String)} treats {@code
 * null} as not binary so 11g/21c document-text redo still assembles. Official 3.7.1.Final calls
 * {@code event.getInfo().endsWith(...)} and NPEs when {@code INFO} is null.
 *
 * @author Chris Cranford
 */
public final class XmlParserUtils {

    private XmlParserUtils() {}

    /**
     * Returns whether the XML event is serialized using binary format.
     *
     * @param event the event, should not be {@code null}
     * @return {@code true} if the XML document is serialized as binary
     */
    public static boolean isXmlSerializedAsBinary(LogMinerEventRow event) {
        return isXmlSerializedAsBinary(event.getInfo());
    }

    /**
     * Same check as {@link #isXmlSerializedAsBinary(LogMinerEventRow)} when only {@code INFO} is
     * available.
     *
     * <p>{@code [SeaTunnel]} {@code null} is not binary. Official 3.7.1.Final has no String
     * overload.
     */
    public static boolean isXmlSerializedAsBinary(String info) {
        return info != null && info.endsWith("not re-executable");
    }
}
