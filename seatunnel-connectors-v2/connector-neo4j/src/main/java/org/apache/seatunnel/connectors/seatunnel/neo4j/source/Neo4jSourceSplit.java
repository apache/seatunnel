package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.source.SourceSplit;

public class Neo4jSourceSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
