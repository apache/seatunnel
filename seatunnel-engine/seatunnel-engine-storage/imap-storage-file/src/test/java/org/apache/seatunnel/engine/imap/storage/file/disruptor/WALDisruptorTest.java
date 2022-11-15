package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFuture;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;

@EnabledOnOs({LINUX, MAC})
public class WALDisruptorTest {

    private static final Configuration CONF = new Configuration();

    private static final String FILEPATH = "/tmp/orc/WALDisruptorTest/";

    private static WALDisruptor DISRUPTOR;

    @Test
    void testProducerAndConsumer() throws IOException {
        DISRUPTOR = new WALDisruptor(CONF, FILEPATH);
        IMapFileData data;
        for (int i = 0; i < 100; i++) {
            data = IMapFileData.builder()
                .deleted(false)
                .key(("key" + i).getBytes())
                .keyClassName(String.class.getName())
                .value(("value" + i).getBytes())
                .valueClassName(String.class.getName())
                .timestamp(System.nanoTime())
                .build();
            long requestId = RequestFutureCache.getRequestId();
            RequestFutureCache.put(requestId, new RequestFuture());
            DISRUPTOR.tryAppendPublish(data, requestId);
        }
        DISRUPTOR.close();
        int archiveFiles = new Path(FILEPATH).getFileSystem(CONF).listStatus(new Path(FILEPATH + "/archive/")).length;
        Assertions.assertEquals(1, archiveFiles);
    }

    @AfterAll
    public static void afterAll() throws IOException {
        Assertions.assertTrue(new Path(FILEPATH).getFileSystem(CONF).delete(new Path(FILEPATH), true));
    }
}
