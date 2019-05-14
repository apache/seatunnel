package io.github.interestinglab.waterdrop;

import io.github.interestinglab.waterdrop.doctor.ClassLoaderUtils;

import java.net.URL;

/**
 * Doctor
 *   command [findClassJar]... // find jar file location of specific class
 *   command [dignose]...    // waterdrop已经挂了，然后收集一些信息，包括系统信息，启动命令，配置文件，yarn logs
 *   command [detectPerf]... // detect performance issues.(Waterdrop 运行时增加统计信息，结合运行时去查看spark运行时job, stage的信息)
 *   command [benchmark] ... // waterdrop baseline benchmark.
 *
 *   param:
 *   (1) engine: spark, flink.
 */
public class WaterdropDoctor {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello World");

//        URL url = org.apache.hadoop.hdfs.web.HftpFileSystem.class.getProtectionDomain().getCodeSource().getLocation();
//        System.out.println(url);
//        URL url = org.apache.hadoop.fs.FileSystem.class.getProtectionDomain().getCodeSource().getLocation();
//        System.out.println(url);

        String clsName = args[0];
        try {
            URL url = ClassLoaderUtils.getJarURL(clsName);
            System.out.println(url);
        } catch (java.lang.ClassNotFoundException e) {
            System.out.println("class not found in runtime jars: " + clsName);
        }
    }
}
