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

package io.debezium.connector.oracle.logminer.events;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

import java.time.Instant;

/**
 * An event that represents a write operation of an XML document in the event stream.
 *
 * @author Chris Cranford
 */
public class XmlWriteEvent extends LogMinerEvent {

    private final String xml;
    private final byte[] xmlBytes;
    private final int length;

    public XmlWriteEvent(LogMinerEventRow row, String xml, Integer length) {
        this(row, xml, null, length);
    }

    public XmlWriteEvent(LogMinerEventRow row, byte[] xmlBytes, Integer length) {
        this(row, null, xmlBytes, length);
    }

    private XmlWriteEvent(LogMinerEventRow row, String xml, byte[] xmlBytes, Integer length) {
        super(row);
        this.xml = xml;
        this.xmlBytes = xmlBytes;
        this.length = length;
    }

    public XmlWriteEvent(
            EventType eventType,
            Scn scn,
            TableId tableId,
            String rowId,
            String rsId,
            Instant changeTime,
            String xml,
            Integer length) {
        this(eventType, scn, tableId, rowId, rsId, changeTime, xml, null, length);
    }

    public XmlWriteEvent(
            EventType eventType,
            Scn scn,
            TableId tableId,
            String rowId,
            String rsId,
            Instant changeTime,
            byte[] xmlBytes,
            Integer length) {
        this(eventType, scn, tableId, rowId, rsId, changeTime, null, xmlBytes, length);
    }

    private XmlWriteEvent(
            EventType eventType,
            Scn scn,
            TableId tableId,
            String rowId,
            String rsId,
            Instant changeTime,
            String xml,
            byte[] xmlBytes,
            Integer length) {
        super(eventType, scn, tableId, rowId, rsId, changeTime);
        this.xml = xml;
        this.xmlBytes = xmlBytes;
        this.length = length;
    }

    public String getXml() {
        return xml;
    }

    /** Raw UTF-8 bytes of one out-of-line {@code HEXTORAW} chunk. Null for inline text. */
    public byte[] getXmlBytes() {
        return xmlBytes;
    }

    public Integer getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "XmlWriteEvent{" + "xml='" + xml + '\'' + "} " + super.toString();
    }
}
