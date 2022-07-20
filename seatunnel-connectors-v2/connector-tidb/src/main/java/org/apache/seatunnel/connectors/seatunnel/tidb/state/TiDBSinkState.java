package org.apache.seatunnel.connectors.seatunnel.tidb.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.transaction.xa.Xid;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TiDBSinkState implements Serializable {
    private final Xid xid;

}
