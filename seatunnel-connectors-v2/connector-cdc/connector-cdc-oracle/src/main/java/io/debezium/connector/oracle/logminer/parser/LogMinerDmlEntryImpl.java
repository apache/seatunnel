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

import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.VisibleForMarshalling;

import java.util.Arrays;
import java.util.Objects;

/** This class holds one parsed DML LogMiner record details */
public class LogMinerDmlEntryImpl implements LogMinerDmlEntry {

    private final EventType eventType;
    private final Object[] newValues;
    private final Object[] oldValues;
    private String objectOwner;
    private String objectName;

    @VisibleForMarshalling
    public LogMinerDmlEntryImpl(
            int eventType, Object[] newValues, Object[] oldValues, String owner, String name) {
        this.eventType = EventType.from(eventType);
        this.newValues = newValues;
        this.oldValues = oldValues;
        this.objectOwner = owner;
        this.objectName = name;
    }

    private LogMinerDmlEntryImpl(EventType eventType, Object[] newValues, Object[] oldValues) {
        this.eventType = eventType;
        this.newValues = newValues;
        this.oldValues = oldValues;
    }

    public static LogMinerDmlEntry forInsert(Object[] newColumnValues) {
        return new LogMinerDmlEntryImpl(EventType.INSERT, newColumnValues, new Object[0]);
    }

    public static LogMinerDmlEntry forUpdate(Object[] newColumnValues, Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(EventType.UPDATE, newColumnValues, oldColumnValues);
    }

    public static LogMinerDmlEntry forDelete(Object[] oldColumnValues) {
        return new LogMinerDmlEntryImpl(EventType.DELETE, new Object[0], oldColumnValues);
    }

    public static LogMinerDmlEntry forValuelessDdl() {
        return new LogMinerDmlEntryImpl(EventType.DDL, new Object[0], new Object[0]);
    }

    public static LogMinerDmlEntry forLobLocator(Object[] newColumnValues) {
        // TODO: can that copy be avoided?
        final Object[] oldColumnValues = Arrays.copyOf(newColumnValues, newColumnValues.length);
        return new LogMinerDmlEntryImpl(
                EventType.SELECT_LOB_LOCATOR, newColumnValues, oldColumnValues);
    }

    public static LogMinerDmlEntry forXml(Object[] oldColumnValues) {
        final Object[] newColumnValues = Arrays.copyOf(oldColumnValues, oldColumnValues.length);
        return new LogMinerDmlEntryImpl(EventType.XML_BEGIN, newColumnValues, oldColumnValues);
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Override
    public Object[] getOldValues() {
        return oldValues;
    }

    @Override
    public Object[] getNewValues() {
        return newValues;
    }

    @Override
    public String getObjectOwner() {
        return objectOwner;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public void setObjectName(String name) {
        this.objectName = name;
    }

    @Override
    public void setObjectOwner(String name) {
        this.objectOwner = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogMinerDmlEntryImpl that = (LogMinerDmlEntryImpl) o;
        return eventType == that.eventType
                && Arrays.equals(newValues, that.newValues)
                && Arrays.equals(oldValues, that.oldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, newValues, oldValues);
    }

    @Override
    public String toString() {
        return "{LogMinerDmlEntryImpl={eventType="
                + eventType
                + ",newColumns="
                + newValues
                + ",oldColumns="
                + oldValues
                + "}";
    }
}
