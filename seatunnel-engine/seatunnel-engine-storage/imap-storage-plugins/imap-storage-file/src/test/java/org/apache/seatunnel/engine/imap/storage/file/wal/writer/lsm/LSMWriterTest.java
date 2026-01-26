package org.apache.seatunnel.engine.imap.storage.file.wal.writer.lsm;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.imap.storage.file.wal.IMapFileIterator;
import org.apache.seatunnel.engine.imap.storage.file.wal.WALFileIterator;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class LSMWriterTest {
    interface WriterFactory {
        IFileWriter create() throws Exception;

        String identifier();
    }

    static Stream<WriterFactory> writerFactories() {
        return Stream.of(
                new WriterFactory() {
                    @Override
                    public IFileWriter create() {
                        return new TestCloudWriter(Collections.emptyMap());
                    }

                    @Override
                    public String identifier() {
                        return "cloud";
                    }
                },
                new WriterFactory() {
                    @Override
                    public IFileWriter create() {
                        return new TestHdfsWriter(Collections.emptyMap());
                    }

                    @Override
                    public String identifier() {
                        return "hdfs";
                    }
                });
    }

    @AfterEach
    void tearDown() throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path baseDir = new Path("target/test-writer");
        fs.delete(baseDir, true);
        fs.close();
    }

    @ParameterizedTest
    @MethodSource("writerFactories")
    void testWriteAndFlushSorted(WriterFactory factory) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path baseDir = new Path("target/test-writer/write-" + factory.identifier());
        fs.delete(baseDir, true);
        fs.mkdirs(baseDir);

        IFileWriter writer = factory.create();
        writer.initialize(fs, baseDir, new ProtoStuffSerializer());

        writer.write(
                new IMapFileData(
                        false, "b".getBytes(), "object", "2".getBytes(), "object", 20260101L),
                false);
        writer.write(
                new IMapFileData(
                        false, "a".getBytes(), "object", "1".getBytes(), "object", 20260101L),
                true);
        writer.write(
                new IMapFileData(
                        false, "c".getBytes(), "object", "3".getBytes(), "object", 20260101L),
                true);

        writer.compaction(true);

        FileStatus[] files =
                fs.listStatus(baseDir, path -> path.getName().startsWith("compaction"));
        Assertions.assertEquals(1, files.length);

        List<IMapFileData> result = readAll(fs, files[0].getPath());

        Assertions.assertEquals("a", new String(result.get(0).getKey()));
        Assertions.assertEquals("b", new String(result.get(1).getKey()));
        Assertions.assertEquals("c", new String(result.get(2).getKey()));

        writer.close();
    }

    @ParameterizedTest
    @MethodSource("writerFactories")
    void testCompaction(WriterFactory factory) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path baseDir = new Path("target/test-writer/write-" + factory.identifier());
        fs.delete(baseDir, true);
        fs.mkdirs(baseDir);

        IFileWriter writer = factory.create();
        writer.initialize(fs, baseDir, new ProtoStuffSerializer());

        writer.write(
                new IMapFileData(
                        false, "a".getBytes(), "object", "2".getBytes(), "object", 20260101000000L),
                true);
        writer.write(
                new IMapFileData(
                        false, "a".getBytes(), "object", "1".getBytes(), "object", 20260101000001L),
                true);
        writer.compaction(true);

        writer.write(
                new IMapFileData(
                        false, "b".getBytes(), "object", "3".getBytes(), "object", 20260101000002L),
                true);
        writer.compaction(true);
        writer.write(
                new IMapFileData(
                        true, "b".getBytes(), "object", "3".getBytes(), "object", 20260101000003L),
                true);
        writer.compaction(true);

        writer.write(
                new IMapFileData(
                        true, "c".getBytes(), "object", "3".getBytes(), "object", 20260101000004L),
                true);
        writer.compaction(true);
        writer.write(
                new IMapFileData(
                        false, "c".getBytes(), "object", "3".getBytes(), "object", 20260101000005L),
                true);
        writer.compaction(true);

        FileStatus[] files = fs.listStatus(baseDir, path -> !path.getName().startsWith("tmp"));
        Assertions.assertEquals(1, files.length);
        List<IMapFileData> result = readAll(fs, files[0].getPath());

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("a", new String(result.get(0).getKey()));
        Assertions.assertEquals("1", new String(result.get(0).getValue()));
        Assertions.assertEquals("b", new String(result.get(1).getKey()));
        Assertions.assertEquals(true, result.get(1).isDeleted());
        Assertions.assertEquals("3", new String(result.get(1).getValue()));
        Assertions.assertEquals("c", new String(result.get(2).getKey()));
        Assertions.assertEquals(false, result.get(2).isDeleted());
        Assertions.assertEquals("3", new String(result.get(2).getValue()));
    }

    @ParameterizedTest
    @MethodSource("writerFactories")
    void testRecoverFromTmpFile(WriterFactory factory) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path baseDir = new Path("target/test-writer/write-" + factory.identifier());
        fs.delete(baseDir, true);
        fs.mkdirs(baseDir);

        IFileWriter writer = factory.create();

        Path tmp = new Path(baseDir, "tmp_10_wal");
        try (FSDataOutputStream out = fs.create(tmp)) {
            write(out, "b", "2");
            write(out, "a", "1");
            write(out, "c", "3");
        }

        writer.initialize(fs, baseDir, new ProtoStuffSerializer());

        FileStatus[] dataFiles = fs.listStatus(baseDir);
        FileStatus dataFile = null;
        for (FileStatus fileStatus : dataFiles) {
            if (fileStatus.getPath().getName().startsWith("data")) {
                dataFile = fileStatus;
                break;
            }
        }
        Assertions.assertNotNull(dataFile);

        List<IMapFileData> result = readAll(fs, dataFile.getPath());

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("a", new String(result.get(0).getKey()));
    }

    @ParameterizedTest
    @MethodSource("writerFactories")
    void testRecoverIgnoresPartialRecord(WriterFactory factory) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path baseDir = new Path("target/test-writer/extra-recover-partial-" + factory.identifier());
        fs.delete(baseDir, true);
        fs.mkdirs(baseDir);

        // write one full record and one partial record into tmp file
        Path tmp = new Path(baseDir, "tmp_99_wal");
        try (FSDataOutputStream out = fs.create(tmp)) {
            IMapFileData d1 =
                    new IMapFileData(
                            false, "b".getBytes(), "object", "2".getBytes(), "object", 20260101L);
            byte[] s1 = new ProtoStuffSerializer().serialize(d1);
            out.write(WALDataUtils.wrapperBytes(s1));

            IMapFileData d2 =
                    new IMapFileData(
                            false, "a".getBytes(), "object", "1".getBytes(), "object", 20260101L);
            byte[] s2 = new ProtoStuffSerializer().serialize(d2);
            byte[] wrapped2 = WALDataUtils.wrapperBytes(s2);
            // write only half of the second record to simulate corruption/partial write
            int half = wrapped2.length / 2;
            out.write(wrapped2, 0, half);
        }

        IFileWriter writer = factory.create();
        writer.initialize(fs, baseDir, new ProtoStuffSerializer());

        FileStatus[] dataFiles = fs.listStatus(baseDir);
        FileStatus dataFile = null;
        for (FileStatus fileStatus : dataFiles) {
            if (fileStatus.getPath().getName().startsWith("data")) {
                dataFile = fileStatus;
                break;
            }
        }
        Assertions.assertNotNull(dataFile);
        List<IMapFileData> result = readAll(fs, dataFile.getPath());
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("b", new String(result.get(0).getKey()));
    }

    @ParameterizedTest
    @MethodSource("writerFactories")
    void testCloseFlushesUnflushedBatch(WriterFactory factory) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        Path baseDir = new Path("target/test-writer/extra-closeflush-" + factory.identifier());
        fs.delete(baseDir, true);
        fs.mkdirs(baseDir);

        IFileWriter writer = factory.create();
        writer.initialize(fs, baseDir, new ProtoStuffSerializer());

        // write entries without forcing flush
        writer.write(
                new IMapFileData(
                        false, "b".getBytes(), "object", "2".getBytes(), "object", 20260101L),
                false);
        writer.write(
                new IMapFileData(
                        false, "a".getBytes(), "object", "1".getBytes(), "object", 20260101L),
                false);

        writer.close();

        FileStatus[] dataFiles = fs.listStatus(baseDir, path -> path.getName().startsWith("data"));
        Assertions.assertEquals(1, dataFiles.length);

        List<IMapFileData> result = readAll(fs, dataFiles[0].getPath());
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("a", new String(result.get(0).getKey()));
        Assertions.assertEquals("b", new String(result.get(1).getKey()));
    }

    private static void write(FSDataOutputStream out, String key, String value) throws IOException {
        IMapFileData data =
                new IMapFileData(
                        false, key.getBytes(), "object", value.getBytes(), "object", 20260101L);
        byte[] serialized = new ProtoStuffSerializer().serialize(data);
        out.write(WALDataUtils.wrapperBytes(serialized));
    }

    private static List<IMapFileData> readAll(FileSystem fs, Path path) throws Exception {
        List<IMapFileData> result = new ArrayList<>();
        try (IMapFileIterator it = new WALFileIterator(fs.open(path), new ProtoStuffSerializer())) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }
        return result;
    }

    static class TestCloudWriter extends CloudLSMWriter {
        public TestCloudWriter(Map<String, Object> config) {
            super(config);
        }

        @Override
        public String identifier() {
            return "cloud";
        }
    }

    static class TestHdfsWriter extends HdfsLSMWriter {
        public TestHdfsWriter(Map<String, Object> config) {
            super(config);
        }

        @Override
        public String identifier() {
            return "hdfs";
        }
    }
}
