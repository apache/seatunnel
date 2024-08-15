package org.apache.seatunnel.connectors.seatunnel.typesense.client;

import org.junit.jupiter.api.Test;
import org.typesense.api.Client;
import org.typesense.api.Configuration;
import org.typesense.resources.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TypesenseClientTest {

    @Test
    public void test() throws Exception {
        List<Node> nodes = new ArrayList<>();

        nodes.add(new Node("http", "localhost", "8108"));

        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(5), "xyz");
        Client client = new Client(configuration);
        TypesenseClient typesenseClient = new TypesenseClient(client);
        System.out.println(typesenseClient.search("companies", null, 0));
        System.out.println(typesenseClient.search("companies", null, 10));
    }
}
