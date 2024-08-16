package org.apache.seatunnel.connectors.seatunnel.typesense.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig;

import org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.typesense.util.URLParamsConverter;

import org.typesense.api.Client;
import org.typesense.api.Configuration;
import org.typesense.model.ImportDocumentsParameters;
import org.typesense.model.SearchParameters;
import org.typesense.model.SearchResult;
import org.typesense.resources.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TypesenseClient {
    private final Client tsClient;

    TypesenseClient(Client tsClient) {
        this.tsClient = tsClient;
    }

    public static TypesenseClient createInstance(ReadonlyConfig config) {
        List<String> hosts = config.get(TypesenseConnectionConfig.HOSTS);
        String protocol = config.get(TypesenseConnectionConfig.protocol);
        String apiKey = config.get(TypesenseConnectionConfig.APIKEY);
        List<Node> nodes = new ArrayList<>();

        hosts.stream()
                .map(host -> host.split(":"))
                .forEach(
                        split ->
                                nodes.add(
                                        new Node(
                                                protocol,
                                                split[0],
                                                StringUtils.isBlank(split[1])
                                                        ? "8018"
                                                        : split[1])));

        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(5), apiKey);
        Client client = new Client(configuration);
        return new TypesenseClient(client);
    }

    public static void main(String[] args) throws Exception {}

    public void insert(String collection,List<String> documentList) {
        ImportDocumentsParameters queryParameters = new ImportDocumentsParameters();
        queryParameters.action("upsert");
        String text = "";
        for (String s : documentList) {
            text = text + s + "\n";
        }
//        String documentList = "{\"countryName\": \"India\", \"capital\": \"Washington\", \"gdp\": 5215}\n" +
//                "{\"countryName\": \"Iran\", \"capital\": \"London\", \"gdp\": 5215}";
// Import your document as JSONL string from a file.
        try {
            tsClient.collections(collection).documents().import_(text, queryParameters);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SearchResult search(String collection, String query, int offset) throws Exception {
        SearchParameters searchParameters;
        if(StringUtils.isNotBlank(query)){
            String jsonQuery = URLParamsConverter.convertParamsToJson(query);
            ObjectMapper objectMapper = new ObjectMapper();
            searchParameters = objectMapper.readValue(jsonQuery, SearchParameters.class);
        }else{
            searchParameters = new SearchParameters().q("*");
        }
        log.debug("Typesense query param:{}",searchParameters);
        System.out.println("query: "+JsonUtils.toJsonString(searchParameters));
        searchParameters.offset(offset);
        SearchResult searchResult =
                tsClient.collections(collection).documents().search(searchParameters);
        return searchResult;
    }
}
