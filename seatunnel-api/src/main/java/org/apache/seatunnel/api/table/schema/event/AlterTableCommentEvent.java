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

package org.apache.seatunnel.api.table.schema.event;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;

import lombok.Getter;
import lombok.ToString;

/**
 * Represents an event where the table comment is altered.
 *
 * <p>This event is triggered by DDL statements like: {@code ALTER TABLE t COMMENT = 'new comment'}
 */
@Getter
@ToString(callSuper = true)
public class AlterTableCommentEvent extends AlterTableEvent {

    /** The old comment of the table. May be null if the table had no comment before. */
    private final String oldComment;

    /** The new comment of the table. May be null if the comment is being removed. */
    private final String newComment;

    public AlterTableCommentEvent(
            TableIdentifier tableIdentifier, String oldComment, String newComment) {
        super(tableIdentifier);
        this.oldComment = oldComment;
        this.newComment = newComment;
    }

    /**
     * Creates an AlterTableCommentEvent.
     *
     * @param tableIdentifier the identifier of the table
     * @param oldComment the old comment (may be null)
     * @param newComment the new comment (may be null)
     * @return the event instance
     */
    public static AlterTableCommentEvent of(
            TableIdentifier tableIdentifier, String oldComment, String newComment) {
        return new AlterTableCommentEvent(tableIdentifier, oldComment, newComment);
    }

    @Override
    public EventType getEventType() {
        return EventType.SCHEMA_CHANGE_ALTER_TABLE_COMMENT;
    }
}
