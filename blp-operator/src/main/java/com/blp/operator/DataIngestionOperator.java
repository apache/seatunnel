package com.blp.operator;

import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class DataIngestionOperator {

    private static final String CONFIG_FILE_NAME_SUFFIX = "_config.json";
    private static final String CONFIG_FILE_DIR = "/tmp/blp";
    private static final String FLINK_MAIN_CLASS =
            "org.apache.seatunnel.core.starter.flink.SeaTunnelFlink";
    private static final String FLINK_MAIN_JAR = "seatunnel-flink-15-starter.jar";
    private static final String ENV_VAR_SEATUNNEL_HOME = "SEATUNNEL_HOME";
    private static final String ENV_VAR_FLINK_HOME = "FLINK_HOME";

    private ExecutorService executor = Executors.newFixedThreadPool(5);

    public Boolean prepareSubmit(DataIngestionRequest request, String fileName) {
        // Create directory if it doesn't exist
        File directory = new File(CONFIG_FILE_DIR);
        if (!directory.exists()) {
            directory.mkdirs(); // mkdirs() creates parent directories if needed
        }

        String configFileStr = request.getConfigJson();
        if (Objects.nonNull(configFileStr)) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
                writer.write(configFileStr);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    public Boolean submit(DataIngestionRequest request, String dir) {
        String configFilePath =
                String.format("%s/%s%s", dir, request.getInstanceName(), CONFIG_FILE_NAME_SUFFIX);
        prepareSubmit(request, configFilePath);
        ;
        // submit to the thread pool
        executor.submit(
                () -> {
                    try {
                        doSubmit(configFilePath, request.getInstanceName());
                    } catch (Exception e) {
                        // call the hookurl to change the status to failure with the reason
                        e.printStackTrace();
                    }
                    // call the hookurl to change the status to submitted
                });
        return true;
    }

    public void doSubmit(String configFilePath, String instanceName)
            throws Exception { // Create a thread instance
        // Command to start a new JVM process

        // Create the process builder
        // java8
        ProcessBuilder processBuilder =
                new ProcessBuilder(
                        "java",
                        "-cp",
                        getClasspath(),
                        setLogConfig(),
                        setLogFileName(instanceName),
                        FLINK_MAIN_CLASS);
        processBuilder.command().addAll(Arrays.asList("--config", configFilePath));
        try {
            // Start the process
            Process process = processBuilder.inheritIO().start();

            // Wait for the process to finish
            int exitCode = process.waitFor();

            // Print the exit code
            System.out.println("Process exited with code: " + exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String setLogConfig() {
        return String.format(
                "-Dlog4j2.configurationFile=%s/config/log4j2.properties",
                System.getenv(ENV_VAR_SEATUNNEL_HOME));
    }

    private String setLogFileName(String jobname) {
        return String.format("-Dseatunnel.logs.file_name=%s", jobname);
    }

    private String getClasspath() {
        List<String> jars = new ArrayList<>();
        // flink lib
        List<String> flinkLibJars =
                listJars(String.format("%s/lib", System.getenv(ENV_VAR_FLINK_HOME)));
        jars.addAll(flinkLibJars);
        // seatunnel lib
        List<String> seatunnelLibJars =
                listJars(String.format("%s/lib", System.getenv(ENV_VAR_SEATUNNEL_HOME)));
        jars.addAll(seatunnelLibJars);
        // seatunnel connectors
        List<String> seatunnelConnectorsJars =
                listJars(String.format("%s/connectors", System.getenv(ENV_VAR_SEATUNNEL_HOME)));
        jars.addAll(seatunnelConnectorsJars);
        // starter jar
        jars.add(
                String.format(
                        "%s/starter/%s", System.getenv(ENV_VAR_SEATUNNEL_HOME), FLINK_MAIN_JAR));
        return String.join(":", jars);
    }

    private List<String> listJars(String folderPath) {
        // Create a File object representing the folder
        File folder = new File(folderPath);
        // Get a list of files in the folder
        File[] files = folder.listFiles();

        // Create a list to store the names of JAR files
        List<String> jarFiles = new ArrayList<>();

        // Filter the files to include only JAR files
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().toLowerCase().endsWith(".jar")) {
                    jarFiles.add(file.getAbsolutePath());
                }
            }
        }
        return jarFiles;
    }

    public Boolean submit(DataIngestionRequest request) {
        return submit(request, CONFIG_FILE_DIR);
    }
}
