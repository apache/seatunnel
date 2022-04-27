package org.apache.seatunnel.utils;

import org.apache.seatunnel.command.AbstractCommandArgs;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {

    /**
     * Get the seatunnel config path.
     * In client mode, the path to the config file is directly given by user.
     * In cluster mode, the path to the config file is the `executor path/config file name`.
     *
     * @param args
     * @return
     */
    public static Path getConfigPath(AbstractCommandArgs args) {
        checkNotNull(args, "args");
        checkNotNull(args.getDeployMode(), "deploy mode");
        switch (args.getDeployMode()) {
            case CLIENT:
                return Paths.get(args.getConfigFile());
            case CLUSTER:
                return Paths.get(getFileName(args.getConfigFile()));
            default:
                throw new IllegalArgumentException("Unsupported deploy mode: " + args.getDeployMode());
        }
    }

    /**
     * Get the file name from the given path.
     * e.g. seatunnel/conf/config.conf -> config.conf
     *
     * @param filePath the path to the file
     * @return file name
     */
    private static String getFileName(String filePath) {
        checkNotNull(filePath, "file path");
        return filePath.substring(filePath.lastIndexOf(File.separatorChar) + 1);
    }

    private static <T> void checkNotNull(T arg, String argName) {
        if (arg == null) {
            throw new IllegalArgumentException(argName + " cannot be null");
        }
    }
}
