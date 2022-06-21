package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class PartitionParameter implements Serializable {

    String partitionColumnName;
    Long minValue;
    Long maxValue;
}
