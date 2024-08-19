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
        //        long companies = typesenseClient.collectionDocNum("companies");
        //        typesenseClient.clearIndexData("companies");
        List<String> documentList = new ArrayList<>();
        System.out.println(typesenseClient.collectionExists("123"));
        //        typesenseClient.createCollection("typesense_test_collection");
        //        documentList.add(
        //
        // "{\"id\":\"1603630728=3951101700341677056\",\"flag\":false,\"num_employees\":1603630728,\"company_name\":\"NIzEH\",\"num\":3951101700341677056}");
        //        documentList.add(
        //
        // "{\"id\":\"1371510502=4857425668889575424\",\"flag\":false,\"num_employees\":1371510502,\"company_name\":\"zVvnQ\",\"num\":4857425668889575424}");
        //        typesenseClient.insert("typesense_test_collection", documentList);
        //        System.out.println(companies);
    }

    @Test
    public void collectionExists() {
        List<Node> nodes = new ArrayList<>();

        nodes.add(new Node("http", "localhost", "8108"));

        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(5), "xyz");
        Client client = new Client(configuration);
        TypesenseClient typesenseClient = new TypesenseClient(client);
        //        typesenseClient.collectionExists("compa1nies");

        System.out.println(typesenseClient.collectionList());
    }
}
