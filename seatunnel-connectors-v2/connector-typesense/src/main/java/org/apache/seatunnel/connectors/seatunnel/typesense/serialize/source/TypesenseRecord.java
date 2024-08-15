package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.source;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

@Getter
@ToString
@AllArgsConstructor
public class TypesenseRecord {
    private Map<String, Object> doc;
}
