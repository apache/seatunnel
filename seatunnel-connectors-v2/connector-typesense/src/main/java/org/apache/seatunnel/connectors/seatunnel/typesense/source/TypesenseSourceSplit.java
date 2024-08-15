package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.SourceCollectionInfo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@AllArgsConstructor
public class TypesenseSourceSplit implements SourceSplit {

    private static final long serialVersionUID = -1L;

    private String splitId;

    @Getter private SourceCollectionInfo sourceCollectionInfo;

    @Override
    public String splitId() {
        return splitId;
    }
}
