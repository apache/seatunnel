package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.DriverBuilder;
import org.neo4j.driver.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.neo4j.driver.Values.parameters;

@Slf4j
public class Neo4jSinkWriter implements SinkWriter<SeaTunnelRow, Neo4jCommitInfo, Neo4jState> {

    private final transient Driver driver;

    public Neo4jSinkWriter(DriverBuilder driverBuilder) {
        this.driver = driverBuilder.build();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try (final Session session = driver.session()){
            session.writeTransaction(tx -> {
                final Result run = tx.run("CREATE (a:Person {name: $name, age: $age})",
                        parameters("name", element.getField(0), "age", element.getField(1)));

                final Query query = run.consume().query();
                System.out.println("query = " + query);
                return 1;
            });
        }
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
        driver.close();
    }
}
