package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Test;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.JacksonUtils;
import io.milvus.grpc.QueryResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.dml.QueryParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class MilvusSinkTest {
    ReadonlyConfig pluginConfig = mock(ReadonlyConfig.class);
    MilvusSink milvusSink = new MilvusSink(pluginConfig, null);

    @Test
    void getPluginName() {
        String pluginName = milvusSink.getPluginName();
        assertEquals("Milvus", pluginName);
    }

    @Test
    void count() {
        String collectionName = "test_mul_field_4";
        String uri = "http://localhost:19530";
        String token = "";
        ConnectConfig connectConfig = ConnectConfig.builder().uri(uri).token(token).build();
        MilvusClientV2 milvusClient = new MilvusClientV2(connectConfig);
        milvusClient.loadCollection(
                LoadCollectionReq.builder().collectionName(collectionName).build());
        QueryResp resp =
                milvusClient.query(
                        QueryReq.builder()
                                .filter("")
                                .outputFields(Arrays.asList("count(*)"))
                                .collectionName(collectionName)
                                .build());
        System.out.println(JacksonUtils.toJsonString(resp));
    }

    @Test
    void count2() {
        String collectionName = "test_mul_field_4";
        String uri = "http://localhost:19530";
        String token = "";
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withUri(uri)
                        .withToken(token)
                        //                .withAuthorization(conn.getUserName(), conn.getPassword())
                        //                .withSecure(conn.isSecure())
                        .build();
        MilvusClient milvusServiceClient = new MilvusServiceClient(connectParam).withRetry(1);
        QueryParam queryParam =
                QueryParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                        .withExpr("_id != -1")
                        .withOutFields(Collections.singletonList("count(*)"))
                        .build();
        R<QueryResults> response = milvusServiceClient.query(queryParam);
        QueryResultsWrapper wrapper = new QueryResultsWrapper(response.getData());
        int size = wrapper.getFieldWrapper("_id").getFieldData().size();
        System.out.println(size);
        System.out.println(wrapper.getFieldWrapper("_id").getFieldData().get(0));
        System.out.println(wrapper.getFieldWrapper("_id").getFieldData().get(size - 1));
    }
}
