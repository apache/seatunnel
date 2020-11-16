package io.github.interestinglab.waterdrop.spark.sink;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchImpl {
    private RestHighLevelClient client;

//     public void initClient(String hostName, int port){
//         this.client = new RestHighLevelClient(
//                 RestClient.builder(
//                         new HttpHost(hostName, port, "http")));
//     }

    public void initClient(@NotNull String host){
        if (host.startsWith("http://") && host.length() > 7){
            host = host.substring(7);
        }
        String[] strArray = host.split(":");
        assert strArray.length == 2;
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(strArray[0], Integer.parseInt(strArray[1]), "http")));

    }
    public void initClient(@NotNull List<String> hosts){
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for(int i = 0; i < hosts.size(); ++i){
            String host = hosts.get(i);
            if (host.startsWith("http://") && host.length() > 7){
                host = host.substring(7);
            }
            String[] strArray = host.split(":");
            assert strArray.length == 2;
            httpHosts[i] =  new HttpHost(strArray[0], Integer.parseInt(strArray[1]), "http");
        }
        this.client = new RestHighLevelClient(RestClient.builder(httpHosts));

    }

    public void closeClient() throws IOException {
        if(client != null){
            client.close();
        }
    }

    public boolean createIndex(@NotNull String indexName, String mappings, int numberOfShards, int numberOfReplicas) throws IOException {
        if(indexExists(indexName)){
            System.out.println("index exists, continue run");
            return true;
        }
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.number_of_replicas", numberOfReplicas)
        );

        // TODO mappings must be json format string
        if (mappings != null && !mappings.isEmpty()) {
            request.mapping(mappings, XContentType.JSON);
        }

        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        return acknowledged & shardsAcknowledged;
    }

    public boolean indexExists(@NotNull String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    public boolean deleteIndex(@NotNull String indexName) throws IOException {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteRequest, RequestOptions.DEFAULT);
        return deleteIndexResponse.isAcknowledged();
    }

    /**
     * delete index firstly, then create it
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean truncateIndex(@NotNull String indexName) throws IOException {
        if (!indexExists(indexName)){
            return true;
        }
        // 获取settings & mappings
        GetIndexRequest getRequest = new GetIndexRequest(indexName);
        GetIndexResponse getIndexResponse = client.indices().get(getRequest, RequestOptions.DEFAULT);

        Map<String, Settings> settingsMap = getIndexResponse.getSettings();
        Settings settings = settingsMap.get(indexName);
        Map<String, MappingMetaData> mappingMetaDataMap = getIndexResponse.getMappings();

        MappingMetaData mappingMetaData = mappingMetaDataMap.get(indexName);
        Map<String, Object> mappingsMap = mappingMetaData.getSourceAsMap();

        String mappings = JSON.toJSONString(mappingsMap);

        int numberOfShards = settings.getAsInt("index.number_of_shards", 5);
        int numberOfReplicas = settings.getAsInt("index.number_of_replicas", 1);

        // 删除索引
        boolean deleteStatus = deleteIndex(indexName);
        // 恢复索引
        boolean createRequest = createIndex(indexName, mappings, numberOfShards, numberOfReplicas);

        return createRequest;
    }

    public static void main(String[] args) {
        ElasticsearchImpl es = new ElasticsearchImpl();
        List<String> hosts = new ArrayList<String>();
        hosts.add("http://centos01:19200");
        hosts.add("centos01:19200");
        // es.initClient("centos01", 19200);
        // es.initClient(hosts.get(0));
        es.initClient(hosts);
        try{
            String mappings = "" +
                    "{\n" +
                    "\t\"properties\": {\n" +
                    "\t\t\"id\": {\n" +
                    "\t\t\t\"type\": \"long\"\n" +
                    "\t\t}\n" +
                    "\t}\n" +
                    "}";

            String indexName = "es_java_high_level";
            if(es.indexExists(indexName)){
                es.deleteIndex(indexName);
            }

            boolean status = es.createIndex(indexName, mappings, 5, 1);
            System.out.println(status);

            es.truncateIndex(indexName);
            System.out.println(status);
            es.closeClient();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }
}