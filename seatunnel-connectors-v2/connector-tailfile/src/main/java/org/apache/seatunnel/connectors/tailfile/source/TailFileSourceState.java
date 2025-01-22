package org.apache.seatunnel.connectors.tailfile.source;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TailFileSourceState implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final TailFileSourceState EMPTY = new TailFileSourceState();
}
