package org.apache.seatunnel.connectors.seatunnel.sls.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class SlsCommitInfo implements Serializable {

    private final String data;
}
