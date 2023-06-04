package org.apache.seatunnel.connectors.seatunnel.milvus.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class MilvusOptions implements Serializable {

    private String milvusHost;
    private Integer milvusPort;
    private String collectionName;
    private String partitionField;
    private String userName;
    private String password;
    private String openaiEngine;
    private String openaiApiKey;
    private Integer dimension;
    private String embeddingsFields;

    public MilvusOptions(Config config){
        this.milvusHost = config.getString(MilvusConfig.MILVUS_HOST.key());
        this.milvusPort = config.getInt(MilvusConfig.MILVUS_PORT.key());
        this.collectionName = config.getString(MilvusConfig.COLLECTION_NAME.key());
        this.userName = config.getString(MilvusConfig.USERNAME.key());
        this.password = config.getString(MilvusConfig.PASSWORD.key());

        if (config.hasPath(MilvusConfig.PARTITION_FIELD.key())){
            this.partitionField = config.getString(MilvusConfig.PARTITION_FIELD.key());
        }
        if (config.hasPath(MilvusConfig.OPENAI_ENGINE.key())){
            this.openaiEngine = config.getString(MilvusConfig.OPENAI_ENGINE.key());
        }
        if (config.hasPath(MilvusConfig.OPENAI_API_KEY.key())){
            this.openaiApiKey = config.getString(MilvusConfig.OPENAI_API_KEY.key());
        }
        if (config.hasPath(MilvusConfig.DIMENSION.key())){
            this.dimension = config.getInt(MilvusConfig.DIMENSION.key());
        }
        if (config.hasPath(MilvusConfig.EMBEDDINGS_FIELDS.key())){
            this.embeddingsFields = config.getString(MilvusConfig.EMBEDDINGS_FIELDS.key());
        }

    }




}
