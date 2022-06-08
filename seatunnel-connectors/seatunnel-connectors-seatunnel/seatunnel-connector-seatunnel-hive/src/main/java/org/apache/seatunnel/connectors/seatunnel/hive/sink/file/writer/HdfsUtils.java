package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

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

    public static boolean deleteFile(String file) throws IOException {
        FileSystem hdfsFs = getHdfsFs(file);
        return hdfsFs.delete(new Path(file), true);
    }

    /**
     * rename file
     *
     * @param oldName     old file name
     * @param newName     target file name
     * @param rmWhenExist if this is true, we will delete the target file when it already exists
     * @throws IOException throw IOException
     */
    public static void renameFile(String oldName, String newName, boolean rmWhenExist) throws IOException {
        FileSystem hdfsFs = getHdfsFs(newName);
        LOGGER.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");

        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        if (rmWhenExist) {
            if (fileExist(newName) && fileExist(oldName)) {
                hdfsFs.delete(newPath, true);
            }
        }
        if (!fileExist(newName.substring(0, newName.lastIndexOf("/")))) {
            createDir(newName.substring(0, newName.lastIndexOf("/")));
        }
        LOGGER.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");

        hdfsFs.rename(oldPath, newPath);
    }

    public static boolean createDir(String filePath)
        throws IOException {

        FileSystem hdfsFs = getHdfsFs(filePath);
        Path dfs = new Path(filePath);
        return hdfsFs.mkdirs(dfs);
    }

    public static boolean fileExist(String filePath)
        throws IOException {
        FileSystem hdfsFs = getHdfsFs(filePath);
        Path fileName = new Path(filePath);
        return hdfsFs.exists(fileName);
    }
}
