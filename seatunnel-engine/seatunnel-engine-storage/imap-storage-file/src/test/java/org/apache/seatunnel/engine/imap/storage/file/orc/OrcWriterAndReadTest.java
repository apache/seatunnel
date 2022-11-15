package org.apache.seatunnel.engine.imap.storage.file.orc;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapData;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.io.IOException;
import java.util.List;

@EnabledOnOs({LINUX, MAC})
public class OrcWriterAndReadTest {

    private static Configuration CONF = new Configuration();

    private static Path PATH = new Path("/tmp/orc/OrcWriterAndReadTest1.orc");

    private static Serializer SERIALIZER = new ProtoStuffSerializer();

    @BeforeAll
    public static void beforeAll() throws IOException {
        OrcWriter writer = new OrcWriter(PATH, CONF);
        IMapFileData data;
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            Long value = System.currentTimeMillis();

            data = IMapFileData.builder()
                .deleted(false)
                .key(SERIALIZER.serialize(key))
                .keyClassName(String.class.getName())
                .value(SERIALIZER.serialize(value))
                .valueClassName(String.class.getName())
                .timestamp(System.nanoTime())
                .build();
            writer.write(data);
        }
        writer.close();
    }

    @Test
    void testRead() throws IOException {
        OrcReader reader = new OrcReader(PATH, CONF);
        List<IMapData> datas = reader.queryAll();
        datas.forEach(data -> {
            try {
                SERIALIZER.deserialize(data.getKey(), String.class);
                SERIALIZER.deserialize(data.getValue(), Long.class);
            } catch (IOException e) {
                Assertions.fail(e);
            }
        });
        Assertions.assertEquals(100, datas.size());
    }

    @AfterAll
    public static void afterAll() throws IOException {
        Assertions.assertTrue(PATH.getFileSystem(CONF).delete(PATH, true));
    }
}
