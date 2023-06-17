package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.junit.jupiter.api.BeforeAll;

import java.util.HashMap;
import java.util.Map;

public class HDFSFileCheckpointTest extends AbstractFileCheckPointTest {

    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        Map<String, String> config = new HashMap<>();
        config.put("storage.type", "hdfs");
        config.put("fs.defaultFS", "hdfs://usdp-bing");
        config.put("seatunnel.hadoop.dfs.nameservices", "usdp-bing");
        config.put("seatunnel.hadoop.dfs.ha.namenodes.usdp-bing", "nn1,nn2");
        config.put("seatunnel.hadoop.dfs.namenode.rpc-address.usdp-bing.nn1", "usdp-bing-nn1:8020");
        config.put("seatunnel.hadoop.dfs.namenode.rpc-address.usdp-bing.nn2", "usdp-bing-nn2:8020");
        config.put("seatunnel.hadoop.dfs.client.failover.proxy.provider.usdp-bing", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        STORAGE = new HdfsStorage(config);
        initStorageData();
    }
}

