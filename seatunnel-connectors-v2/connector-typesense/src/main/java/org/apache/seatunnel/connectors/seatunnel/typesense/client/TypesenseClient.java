package org.apache.seatunnel.connectors.seatunnel.typesense.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig;

import org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.typesense.util.URLParamsConverter;

import org.typesense.api.Client;
import org.typesense.api.Collections;
import org.typesense.api.Configuration;
import org.typesense.api.FieldTypes;
import org.typesense.model.CollectionResponse;
import org.typesense.model.CollectionSchema;
import org.typesense.model.DeleteDocumentsParameters;
import org.typesense.model.Field;
import org.typesense.model.ImportDocumentsParameters;
import org.typesense.model.SearchParameters;
import org.typesense.model.SearchResult;
import org.typesense.resources.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public boolean collectionExists(String collection){
        try {
            tsClient.collections(collection).retrieve();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    public List<String> collectionList(){
        try {
            Collections collections = tsClient.collections();
            CollectionResponse[] collectionResponses = collections.retrieve();
            List<String> list = new ArrayList<>();
            for (CollectionResponse collectionRespons : collectionResponses) {
                String collectionName = collectionRespons.getName();
                list.add(collectionName);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, BasicTypeDefine<TypesenseType>> getFieldTypeMapping(
            String collection) {
        Map<String, BasicTypeDefine<TypesenseType>> allElasticSearchFieldTypeInfoMap = new HashMap<>();

        try {
            CollectionResponse collectionResponse = tsClient.collections(collection).retrieve();
            List<Field> fields = collectionResponse.getFields();
            for (Field field : fields) {
                String fieldName = field.getName();
                String type = field.getType();
                BasicTypeDefine.BasicTypeDefineBuilder<TypesenseType> typeDefine =
                        BasicTypeDefine.<TypesenseType>builder()
                                .name(fieldName)
                                .columnType(type)
                                .dataType(type)
                                .nativeType(new TypesenseType(type, new HashMap<>()));
                allElasticSearchFieldTypeInfoMap.put(fieldName, typeDefine.build());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public boolean createCollection(String collection){
        CollectionSchema collectionSchema = new CollectionSchema();
        List<Field> fields = new ArrayList<>();
        fields.add(new Field().name("_update_time").type(FieldTypes.STRING));
        collectionSchema.name(collection).fields(fields);
        try {
            System.out.println(tsClient.collections().create(collectionSchema));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean dropCollection(String collection){
        try {
            tsClient.collections(collection).delete();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean clearIndexData(String collection){
        DeleteDocumentsParameters deleteDocumentsParameters = new DeleteDocumentsParameters();
        deleteDocumentsParameters.filterBy("id:!=1||id:=1");
        try {
            tsClient.collections(collection).documents().delete(deleteDocumentsParameters);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public long collectionDocNum(String collection) {
        SearchParameters q = new SearchParameters().q("*");
        try {
            SearchResult searchResult = tsClient.collections(collection).documents().search(q);
            return searchResult.getFound();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }
}
