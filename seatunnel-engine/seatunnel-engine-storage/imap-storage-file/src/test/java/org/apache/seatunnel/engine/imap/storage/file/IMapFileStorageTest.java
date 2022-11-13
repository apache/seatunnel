package org.apache.seatunnel.engine.imap.storage.file;

import org.apache.seatunnel.engine.imap.storage.file.orc.OrcReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IMapFileStorageTest {


    public static void main(String[] args) throws InterruptedException {


        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://localhost:9000/");
        config.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        System.out.println(System.nanoTime());
        System.out.println(System.currentTimeMillis());
        Map<String, Object> props = new HashMap<>();
        props.put("namespace", "file//test");
        props.put("businessName", "kris");
        props.put("clusterName", "cluster1");
        props.put("hadoopConf", config);

        IMapFileStorage storage = new IMapFileStorage();
        storage.initialize(props);
        
         /*for (int i=0;i<1000;i++){
             String key = "key"+i;
             Long value = Long.valueOf(i);
            System.out.println(storage.store(key,value)+" "+i);
        }
         System.out.println(storage.archive()+"archive");*/
       storage.loadAll().forEach((key, value) -> {
            System.out.println(key + " " + value); 
        });
        
    }
}
