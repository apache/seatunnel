package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsUtils {
    public static final int WRITE_BUFFER_SIZE = 2048;

    public static FileSystem getHdfsFs(String path)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFs", path);
        return FileSystem.get(conf);
    }

    public static FSDataOutputStream getOutputStream(String outFilePath) throws IOException {
        FileSystem hdfsFs = getHdfsFs(outFilePath);
        Path path = new Path(outFilePath);
        FSDataOutputStream fsDataOutputStream = hdfsFs.create(path, true, WRITE_BUFFER_SIZE);
        return fsDataOutputStream;
    }
}
