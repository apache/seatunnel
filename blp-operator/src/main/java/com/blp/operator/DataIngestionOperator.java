package com.blp.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
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

    private final Logger log = LoggerFactory.getLogger(DataIngestionOperator.class);

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
                        doSubmit(configFilePath, request);
                    } catch (Exception e) {
                        // call the hookurl to change the status to failure with the reason
                        e.printStackTrace();
                    }
                    // call the hookurl to change the status to submitted
                });
        return true;
    }

    public void doSubmit(String configFilePath, DataIngestionRequest request)
            throws Exception { // Create a thread instance
        // Command to start a new JVM process
        // Create the process builder
        // java8
        String flinkHome = System.getenv(ENV_VAR_FLINK_HOME);
        String seatunnelHome = System.getenv(ENV_VAR_SEATUNNEL_HOME);
        ProcessBuilder processBuilder =
                new ProcessBuilder(
                        String.format("%s/bin/flink", flinkHome),
                        "run",
                        "-c",
                        FLINK_MAIN_CLASS,
                        String.format("%s/starter/%s", seatunnelHome, FLINK_MAIN_JAR),
                        "--name",
                        request.getInstanceName());
        processBuilder.command().addAll(Arrays.asList("--config", configFilePath));
        log.info("blp start cmd: " + String.join(" ", processBuilder.command()));
        try {
            // Redirect output to the output file
            File outputFile =
                    new File(System.getProperty("java.io.tmpdir") + "/" + request.instanceName);
            log.info(
                    String.format(
                            "instance:%s submit log:%s",
                            request.getInstanceName(), outputFile.getAbsolutePath()));
            processBuilder.redirectOutput(outputFile);
            // Start the process
            Process process = processBuilder.start();

            // Wait for the process to finish
            int exitCode = process.waitFor();

            // Print the exit code
            if (exitCode != 0) {
                log.error("Process exited with code: " + exitCode);
            } else {
                log.info("Process exited with code: " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public Boolean submit(DataIngestionRequest request) {
        return submit(request, CONFIG_FILE_DIR);
    }
}
