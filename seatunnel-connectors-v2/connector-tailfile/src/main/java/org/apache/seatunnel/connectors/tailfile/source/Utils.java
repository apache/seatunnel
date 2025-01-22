package org.apache.seatunnel.connectors.tailfile.source;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Files;

@Slf4j
public class Utils {

    public static long getInode(File file) throws IOException {
        if (!file.isFile()) {
            throw new UnsupportedOperationException("Unsupported path: " + file.getAbsolutePath());
        }
        return (long) Files.getAttribute(file.toPath(), "unix:ino");
    }

    public static String getHostname() {
        try {
            Process process = Runtime.getRuntime().exec("hostname");
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String hostname = reader.readLine();
                if (hostname != null) {
                    return hostname;
                }
            }

            InetAddress inetAddress = InetAddress.getLocalHost();
            return inetAddress.getHostName();
        } catch (IOException e) {
            log.error("Failed to get hostname", e);
            return null;
        }
    }

    public static String getIpAddress() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return inetAddress.getHostAddress();
        } catch (IOException e) {
            log.error("Failed to get hostname", e);
            return null;
        }
    }

    public static File getParentDir(String... paths) {
        return null;
    }

    public static File getParentDir(String path) {
        File parentDir = new File(path).getParentFile();
        if (parentDir.exists()) {
            return parentDir;
        }

        String[] splits = path.split("/");
        for (int i = splits.length - 1; i >= 0; i--) {
            String[] parentSplits = new String[i];
            System.arraycopy(splits, 0, parentSplits, 0, i);
            String parentPath = String.join("/", parentSplits);
            parentDir = new File(parentPath);
            if (parentDir.exists()) {
                return parentDir;
            }
        }
        throw new IllegalArgumentException("Parent directory does not exist: " + path);
    }
}
