package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;

import java.util.Map;

@AutoService(CheckpointStorageFactory.class)
public class HdfsStorageFactory implements CheckpointStorageFactory {
    @Override
    public String name() {
        return "hdfs";
    }

    @Override
    public CheckpointStorage create(Map<String, String> configuration) {
        return null;
    }
}
