package org.apache.seatunnel.api.table.event;

import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.ToString;

@ToString(callSuper = true)
public abstract class AlterTableColumnEvent extends AlterTableEvent {
    public AlterTableColumnEvent(TablePath tablePath) {
        super(tablePath);
    }
}
