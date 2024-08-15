package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
public class TypesenseSourceState implements Serializable {
    private boolean shouldEnumerate;
    private Map<Integer, List<TypesenseSourceSplit>> pendingSplit;
}
