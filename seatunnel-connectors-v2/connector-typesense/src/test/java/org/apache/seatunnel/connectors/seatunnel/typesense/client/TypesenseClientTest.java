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
    public void search() throws Exception {
        List<Node> nodes = new ArrayList<>();

        nodes.add(new Node("http", "localhost", "8108"));

        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(5), "xyz");
        Client client = new Client(configuration);
        TypesenseClient typesenseClient = new TypesenseClient(client);
        String query = "q=*&filter_by=num_employees:>9000";
//        System.out.println(typesenseClient.search("companies", null, 0));
//        typesenseClient.search("companies", query, 0);
//        System.out.println(typesenseClient.search("companies", query, 0));
//        typesenseClient.getFieldTypeMapping("companies");
//        typesenseClient.createCollection("test");
        long companies = typesenseClient.collectionDocNum("companies");
        typesenseClient. clearIndexData("companies");
        System.out.println(companies);
    }

    @Test
    public void collectionExists(){
        List<Node> nodes = new ArrayList<>();

        nodes.add(new Node("http", "localhost", "8108"));

        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(5), "xyz");
        Client client = new Client(configuration);
        TypesenseClient typesenseClient = new TypesenseClient(client);
//        typesenseClient.collectionExists("compa1nies");

        System.out.println(typesenseClient.collectionList());
    }
}
