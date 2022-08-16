package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jConfig;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class Neo4jSinkWriter implements SinkWriter<SeaTunnelRow, Neo4jCommitInfo, Neo4jState> {

    private final Neo4jConfig config;
    private final transient Driver driver;
    private final transient Session session;

    public Neo4jSinkWriter(Neo4jConfig neo4jConfig) {
        this.config = neo4jConfig;
        this.driver = config.getDriverBuilder().build();
        this.session = driver.session();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        final Map<String, Object> queryParamPosition = config.getQueryParamPosition().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> element.getField((Integer) e.getValue())));
        final Query query = new Query(config.getQuery(), queryParamPosition);
        session.writeTransaction(tx -> {
            tx.run(query);
            return null;
        });
    }

    @Override
    public Optional<Neo4jCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public List<Neo4jState> snapshotState(long checkpointId) throws IOException {
        return SinkWriter.super.snapshotState(checkpointId);
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {
        session.close();
        driver.close();
    }
}
