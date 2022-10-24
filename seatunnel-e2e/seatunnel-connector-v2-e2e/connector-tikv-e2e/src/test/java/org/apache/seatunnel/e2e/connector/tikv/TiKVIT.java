package org.apache.seatunnel.e2e.connector.tikv;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.ClientSession;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVDataType;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.DockerComposeContainer;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import scala.Tuple2;

@Slf4j
public class TiKVIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_COMPOSE_PARCH = "/pd_tikv_container_compose.yml";
    private static final String HOST = "localhost";
    private static final Integer PD_PORT = 2379;
    private ClientSession clientSession;
    private RawKVClient client;

    private static final Tuple2<SeaTunnelRowType, List<SeaTunnelRow>> TEST_DATASET = generateTestDataSet();

    public static String getTestConfigFilePath(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = TiKVIT.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

    /**
     * tidb starting sequence ==> Placement Driver (PD) -> TiKV -> TiDB
     */
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("------------------------------- startUp ---------------------------------");
        log.info("TiKV container started...");
        new DockerComposeContainer(new File(getTestConfigFilePath(DOCKER_COMPOSE_PARCH)))
            .withEnv("network_mode", "host")
            .start();
        log.info("------------------------------- init client session ---------------------------------");
        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initClientSession);
        log.info("------------------------------- int source data ---------------------------------");
        this.initSourceData();
    }

    private void initClientSession() {
        // init TiKV Parameters
        TiKVParameters tikvParameters = new TiKVParameters();
        tikvParameters.setHost(HOST);
        tikvParameters.setPdPort(PD_PORT);
        tikvParameters.setKeyword("k1");
        tikvParameters.setTikvDataType(TiKVDataType.KEY);
        tikvParameters.setLimit(1000);

        this.clientSession = new ClientSession(tikvParameters);
        this.client = this.clientSession.session.createRawClient();
    }

    private void initSourceData() {
        JsonSerializationSchema jsonSerializationSchema = new JsonSerializationSchema(TEST_DATASET._1());
        List<SeaTunnelRow> rows = TEST_DATASET._2();
        for (int i = 0; i < rows.size(); i++) {
            client.put(ByteString.copyFromUtf8("k" + i), ByteString.copyFromUtf8(rows.get(i).getField(0).toString()));
        }

    }

    @AfterEach
    public void close() {
        try {
            clientSession.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Tuple2<SeaTunnelRowType, List<SeaTunnelRow>> generateTestDataSet() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(
            new String[]{"id"},
            new SeaTunnelDataType[]{BasicType.LONG_TYPE}
        );

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[]{(long) i});
            rows.add(row);
        }
        return Tuple2.apply(rowType, rows);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            clientSession.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    public void testTiKV(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/tikv-to-tikv.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(100, client.scanPrefix(ByteString.copyFromUtf8("k")).size());
        // Clear data to prevent data duplication in the next TestContainer
        client.deletePrefix(ByteString.copyFromUtf8("k"));
        Assertions.assertEquals(0, client.scanPrefix(ByteString.copyFromUtf8("k")).size());
    }
}
