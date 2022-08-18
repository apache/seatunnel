/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.oss.fs;

import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_CORE_POOL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_ESTABLISH_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_KEEP_ALIVE_TIME;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MAXIMUM_CONNECTIONS;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MAX_ERROR_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MAX_POOL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MINIMUM_COPY_PART_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MINIMUM_UPLOAD_PART_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MULTIPART_COPY_THRESHOLD;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_MULTIPART_UPLOAD_THRESHOLD;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_SOCKET_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DEFAULT_USER_AGENT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.DELETE_OBJECTS_ONETIME_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_CORE_POOL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_ESTABLISH_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_KEEP_ALIVE_TIME;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MAXIMUM_CONNECTIONS;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MAX_ERROR_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MAX_POOL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MULTIPART_COPY_PART_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MULTIPART_COPY_THRESHOLD;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MULTIPART_UPLOAD_PART_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_MULTIPART_UPLOAD_THRESHOLD;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_PROXY_DOMAIN;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_PROXY_HOST;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_PROXY_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_PROXY_PORT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_PROXY_USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_PROXY_WORKSTATION;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_SECURE_CONNECTIONS;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.HADOOP_SOCKET_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.MAX_RETURNED_KEYS_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.file.oss.fs.SmartOSSClientConfig.OSS_DEFAULT_BLOCK_SIZE;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.event.ProgressEvent;
import com.aliyun.oss.event.ProgressListener;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Hadoop File System implementation for Aliyun OSS.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OSSFileSystem extends FileSystem {

    private URI uri;
    private Path workingDir;
    private String bucket;
    private OSSClient client;

    public static final Logger LOG = LoggerFactory.getLogger(OSSFileSystem.class);

    /**
     * Called after a new FileSystem instance is constructed.
     *
     * @param name URI
     * @param conf configuration
     * @throws IOException IOException
     */
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);

        uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this.uri,
                this.getWorkingDirectory());

        // Try to get our credentials or just connect anonymously
        String accessKeyId = conf.get(SmartOSSClientConfig.HADOOP_ACCESS_KEY, null);
        String accessKeySecret = conf.get(SmartOSSClientConfig.HADOOP_SECRET_KEY, null);
        String endpoint = conf.get(SmartOSSClientConfig.HADOOP_ENDPOINT, null);

        bucket = name.getHost();

        // Initialize OSS Client, please refer to help.aliyun.com/document_detail/oss/sdk/java-sdk/init.html
        // for the detailed information.
        SmartOSSClientConfig ossConf = new SmartOSSClientConfig();
        //user agent
        ossConf.setUserAgent(conf.getTrimmed(HADOOP_PROXY_HOST, DEFAULT_USER_AGENT));
        //connect to oss through a proxy server
        String proxyHost = conf.getTrimmed(HADOOP_PROXY_HOST, "");
        int proxyPort = conf.getInt(HADOOP_PROXY_PORT, -1);
        String proxyUsername = conf.getTrimmed(HADOOP_PROXY_USERNAME);
        String proxyPassword = conf.getTrimmed(HADOOP_PROXY_PASSWORD);
        if (!proxyHost.isEmpty() && proxyPort >= 0) {
            ossConf.setProxyHost(proxyHost);
            ossConf.setProxyPort(proxyPort);
        }
        if (proxyUsername != null) {
            ossConf.setProxyUsername(proxyUsername);
        }
        if (proxyPassword != null) {
            ossConf.setProxyPassword(proxyPassword);
        }
        ossConf.setProxyDomain(conf.getTrimmed(HADOOP_PROXY_DOMAIN));
        ossConf.setProxyWorkstation(conf.getTrimmed(HADOOP_PROXY_WORKSTATION));
        //MaxConnections
        ossConf.setMaxConnections(conf.getInt(HADOOP_MAXIMUM_CONNECTIONS, DEFAULT_MAXIMUM_CONNECTIONS));
        //SocketTimeout
        ossConf.setSocketTimeout(conf.getInt(HADOOP_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT));
        //ConnectionTimeout
        ossConf.setConnectionTimeout(conf.getInt(HADOOP_ESTABLISH_TIMEOUT, DEFAULT_ESTABLISH_TIMEOUT));
        //MaxErrorRetry
        ossConf.setMaxErrorRetry(conf.getInt(HADOOP_MAX_ERROR_RETRIES, DEFAULT_MAX_ERROR_RETRIES));
        //Protocol
        boolean secureConnections = conf.getBoolean(HADOOP_SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);
        ossConf.setProtocol(secureConnections ? Protocol.HTTPS : Protocol.HTTP);
        //SupportCname
        ossConf.setSupportCname(false);  //ListBuckets can not be used when enable CNAME.
        ossConf.setSLDEnabled(false);

        //extra configuration for multiple part copy/upload
        ossConf.setMultipartUploadThreshold(conf.getLong(HADOOP_MULTIPART_UPLOAD_THRESHOLD, DEFAULT_MULTIPART_UPLOAD_THRESHOLD));
        ossConf.setMinimumUploadPartSize(conf.getLong(HADOOP_MULTIPART_UPLOAD_PART_SIZE, DEFAULT_MINIMUM_UPLOAD_PART_SIZE));
        ossConf.setMultipartCopyThreshold(conf.getLong(HADOOP_MULTIPART_COPY_THRESHOLD, DEFAULT_MULTIPART_COPY_THRESHOLD));
        ossConf.setMultipartCopyPartSize(conf.getLong(HADOOP_MULTIPART_COPY_PART_SIZE, DEFAULT_MINIMUM_COPY_PART_SIZE));
        // extra configuration for multiple part copy/upload thread pool
        ossConf.setCorePoolSize(conf.getInt(HADOOP_CORE_POOL_SIZE, DEFAULT_CORE_POOL_SIZE));
        ossConf.setMaxPoolSize(conf.getInt(HADOOP_MAX_POOL_SIZE, DEFAULT_MAX_POOL_SIZE));
        ossConf.setKeepAliveTime(conf.getInt(HADOOP_KEEP_ALIVE_TIME, DEFAULT_KEEP_ALIVE_TIME));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Using proxy server {}:{} as user {} with password {} on " +
                            "domain {} as workstation {}", ossConf.getProxyHost(),
                    ossConf.getProxyPort(), ossConf.getProxyUsername(),
                    ossConf.getProxyPassword(), ossConf.getProxyDomain(),
                    ossConf.getProxyWorkstation());
        }

        client = new SmartOSSClient(endpoint, accessKeyId, accessKeySecret, ossConf);
        if (!client.doesBucketExist(bucket)) {
            throw new IOException("Bucket " + bucket + " does not exist");
        }
        setConf(conf);

    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return "oss"
     */
    public String getScheme() {
        return "oss";
    }

    /**
     * Returns a URI whose scheme and authority identify this FileSystem.
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Returns the OSS client used by this filesystem.
     *
     * @return oss client
     */
    OSSClient getOSSClient() {
        return client;
    }

    public OSSFileSystem() {
        super();
    }

    /**
     * Turns a path (relative or otherwise) into an OSS key
     *
     * @param path path
     * @return the Path object
     */
    private String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }
        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            return "";
        }
        return path.toUri().getPath().substring(1);
    }

    private Path keyToPath(String key) {
        return new Path("/" + key);
    }

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f          the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    public FSDataInputStream open(Path f, int bufferSize)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening '{}' for reading.", f);
        }
        final FileStatus fileStatus = getFileStatus(f);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + f + " because it is a directory");
        }

        return new FSDataInputStream(new OSSInputStream(bucket, pathToKey(f),
                fileStatus.getLen(), client, statistics));
    }

    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        String key = pathToKey(f);

        if (!overwrite && exists(f)) {
            throw new FileAlreadyExistsException(f + " already exists");
        }
        // We pass null to FSDataOutputStream, so it won't count writes that are being buffered to a file
        return new FSDataOutputStream(new OSSOutputStream(getConf(), this,
                bucket, key, progress, statistics), null);
    }

    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }

    public boolean rename(Path src, Path dst) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Rename path {} to {}", src, dst);
        }

        String srcKey = pathToKey(src);
        String dstKey = pathToKey(dst);

        if (srcKey.isEmpty() || dstKey.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("rename: src or dst are empty");
            }
            return false;
        }

        OSSFileStatus srcStatus;
        try {
            srcStatus = getFileStatus(src);
        } catch (FileNotFoundException e) {
            LOG.error("rename: src not found {}", src);
            return false;
        }

        if (srcKey.equals(dstKey)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("rename: src and dst refer to the same file or directory");
            }
            return srcStatus.isFile();
        }

        OSSFileStatus dstStatus = null;
        try {
            dstStatus = getFileStatus(dst);

            if (srcStatus.isDirectory() && dstStatus.isFile()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("rename: src is a directory and dst is a file");
                }
                return false;
            }

            if (dstStatus.isDirectory() && !dstStatus.isEmptyDirectory()) {
                return false;
            }
        } catch (FileNotFoundException e) {
            // Parent must exist
            Path parent = dst.getParent();
            if (!pathToKey(parent).isEmpty()) {
                try {
                    OSSFileStatus dstParentStatus = getFileStatus(dst.getParent());
                    if (!dstParentStatus.isDirectory()) {
                        return false;
                    }
                } catch (FileNotFoundException e2) {
                    return false;
                }
            }
        }

        // Ok! Time to start
        if (srcStatus.isFile()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("rename: renaming file " + src + " to " + dst);
            }
            if (dstStatus != null && dstStatus.isDirectory()) {
                String newDstKey = dstKey;
                if (!newDstKey.endsWith("/")) {
                    newDstKey = newDstKey + "/";
                }
                String filename =
                        srcKey.substring(pathToKey(src.getParent()).length() + 1);
                newDstKey = newDstKey + filename;
                copyFile(srcKey, newDstKey);
            } else {
                copyFile(srcKey, dstKey);
            }
            delete(src, false);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("rename: renaming directory " + src + " to " + dst);
            }

            // This is a directory to directory copy
            if (!dstKey.endsWith("/")) {
                dstKey = dstKey + "/";
            }

            if (!srcKey.endsWith("/")) {
                srcKey = srcKey + "/";
            }

            //Verify dest is not a child of the source directory
            if (dstKey.startsWith(srcKey)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("cannot rename a directory to a subdirectory of self");
                }
                return false;
            }

            List<String> keysToDelete =
                    new ArrayList<String>();
            if (dstStatus != null && dstStatus.isEmptyDirectory()) {
                // delete unnecessary fake directory.
                keysToDelete.add(dstKey);
            }

            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(srcKey);
            request.setMaxKeys(MAX_RETURNED_KEYS_LIMIT);

            ObjectListing objects = client.listObjects(request);
            statistics.incrementReadOps(1);

            String nextMarker;
            while (true) {
                for (OSSObjectSummary summary : objects.getObjectSummaries()) {
                    keysToDelete.add(summary.getKey());
                    String newDstKey = dstKey + summary.getKey().substring(srcKey.length());
                    copyFile(summary.getKey(), newDstKey);

                    if (keysToDelete.size() == DELETE_OBJECTS_ONETIME_LIMIT) {
                        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
                        client.deleteObjects(deleteRequest);
                        statistics.incrementWriteOps(1);
                        keysToDelete.clear();
                    }
                }

                if (objects.isTruncated()) {
                    nextMarker = objects.getNextMarker();
                    objects = client.listObjects(request.withMarker(nextMarker));
                    statistics.incrementReadOps(1);
                } else {
                    if (keysToDelete.size() > 0) {
                        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
                        client.deleteObjects(deleteRequest);
                        statistics.incrementWriteOps(1);
                    }
                    break;
                }
            }
        }

        if (src.getParent() != dst.getParent()) {
            deleteUnnecessaryEmptyDirectories(dst.getParent());
            createEmptyDirectoryIfNecessary(src.getParent());
        }
        return true;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Delete path " + f + " - recursive " + recursive);
        }
        OSSFileStatus status;
        try {
            status = getFileStatus(f);
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Couldn't delete " + f + " - does not exist");
            }
            return false;
        }

        String key = pathToKey(f);

        if (status.isDirectory()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete: Path is a directory");
            }

            if (!recursive && !status.isEmptyDirectory()) {
                throw new IOException("Path is a folder: " + f + " and it is not an empty directory");
            }

            if (!key.endsWith("/")) {
                key = key + "/";
            }

            if (key.equals("/")) {
                LOG.info("oss cannot delete the root directory");
                return false;
            }

            if (status.isEmptyDirectory()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Deleting empty directory");
                }
                client.deleteObject(bucket, key);
                statistics.incrementWriteOps(1);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Getting objects for directory prefix " + key + " to delete");
                }

                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(bucket);
                request.setPrefix(key);
                // Hopefully not setting a delimiter will cause this to find everything
                //request.setDelimiter("/");
                request.setMaxKeys(MAX_RETURNED_KEYS_LIMIT);

                List<String> keys = new ArrayList<String>();
                ObjectListing objects = client.listObjects(request);
                statistics.incrementReadOps(1);

                String nextMarker;
                while (true) {
                    for (OSSObjectSummary summary : objects.getObjectSummaries()) {
                        keys.add(summary.getKey());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Got object to delete " + summary.getKey());
                        }

                        if (keys.size() == DELETE_OBJECTS_ONETIME_LIMIT) {
                            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
                            client.deleteObjects(deleteRequest);
                            statistics.incrementWriteOps(1);
                            keys.clear();
                        }
                    }

                    if (objects.isTruncated()) {
                        nextMarker = objects.getNextMarker();
                        objects = client.listObjects(request.withMarker(nextMarker));
                        statistics.incrementReadOps(1);
                    } else {
                        if (!keys.isEmpty()) {
                            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keys);
                            client.deleteObjects(deleteRequest);
                            statistics.incrementWriteOps(1);
                        }
                        break;
                    }
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete: Path is a file");
            }
            client.deleteObject(bucket, key);
            statistics.incrementWriteOps(1);
        }

        createEmptyDirectoryIfNecessary(f.getParent());

        return true;
    }

    private void createEmptyDirectoryIfNecessary(Path f) throws IOException {
        String key = pathToKey(f);
        if (!key.isEmpty() && !exists(f)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Creating new empty directory at " + f);
            }
            createEmptyDirectory(bucket, key);
        }
    }

    /**
     * List the statuses of the files/directories in the given path if the path is
     * a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given patch
     * @throws FileNotFoundException when the path does not exist;
     *                               IOException see specific implementation
     */
    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
            IOException {
        String key = pathToKey(f);
        if (LOG.isDebugEnabled()) {
            LOG.debug("List status for path: " + f);
        }

        final List<FileStatus> result = new ArrayList<FileStatus>();
        final FileStatus fileStatus = getFileStatus(f);

        if (fileStatus.isDirectory()) {
            if (!key.isEmpty()) {
                key = key + "/";
            }

            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(key);
            request.setDelimiter("/");
            request.setMaxKeys(MAX_RETURNED_KEYS_LIMIT);

            if (LOG.isDebugEnabled()) {
                LOG.debug("listStatus: doing listObjects for directory " + key);
            }

            ObjectListing objects = client.listObjects(request);
            statistics.incrementReadOps(1);

            String nextMarker;
            while (true) {
                for (OSSObjectSummary summary : objects.getObjectSummaries()) {
                    Path keyPath = keyToPath(summary.getKey()).makeQualified(uri, workingDir);
                    if (keyPath.equals(f)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring: " + keyPath);
                        }
                        continue;
                    }

                    if (objectRepresentsDirectory(summary.getKey(), summary.getSize())) {
                        result.add(new OSSFileStatus(true, true, keyPath));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Adding: fd: " + keyPath);
                        }
                    } else {
                        result.add(new OSSFileStatus(summary.getSize(),
                                dateToLong(summary.getLastModified()), keyPath,
                                getDefaultBlockSize(f.makeQualified(uri, workingDir))));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Adding: fi: " + keyPath);
                        }
                    }
                }

                for (String prefix : objects.getCommonPrefixes()) {
                    Path keyPath = keyToPath(prefix).makeQualified(uri, workingDir);
                    if (keyPath.equals(f)) {
                        continue;
                    }
                    result.add(new OSSFileStatus(true, false, keyPath));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Adding: rd: " + keyPath);
                    }
                }

                if (objects.isTruncated()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("listStatus: list truncated - getting next batch");
                    }
                    nextMarker = objects.getNextMarker();
                    objects = client.listObjects(request.withMarker(nextMarker));
                    statistics.incrementReadOps(1);
                } else {
                    break;
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding: rd (not a dir): " + f);
            }
            result.add(fileStatus);
        }

        return result.toArray(new FileStatus[result.size()]);
    }

    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    /**
     * Get the current working directory for the given file system
     *
     * @return the directory pathname
     */
    public Path getWorkingDirectory() {
        return workingDir;
    }

    /**
     * Make the given file and all non-existent parents into
     * directories. Has the semantics of Unix 'mkdir -p'.
     * Existence of the directory hierarchy is not an error.
     *
     * @param f          path to create
     * @param permission to apply to f
     */
    // TODO: If we have created an empty file at /foo/bar and we then call
    // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Making directory: " + f);
        }
        try {
            FileStatus fileStatus = getFileStatus(f);

            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + f);
            }
        } catch (FileNotFoundException e) {

            Path fPart = f;
            do {
                try {
                    FileStatus fileStatus = getFileStatus(fPart);
                    if (fileStatus.isFile()) {
                        throw new FileAlreadyExistsException(String.format(
                                "Can't make directory for path '%s' since it is a file.",
                                fPart));
                    }
                } catch (FileNotFoundException fileNotFoundException) {
                    LOG.error(fileNotFoundException.getMessage());
                }
                fPart = fPart.getParent();
            } while (fPart != null);

            String key = pathToKey(f);
            createEmptyDirectory(bucket, key);
            return true;
        }
    }

    /**
     * Return a file status object that represents the path.
     *
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist;
     *                               IOException see specific implementation
     */
    public OSSFileStatus getFileStatus(Path f) throws IOException {
        String key = pathToKey(f);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting path status for " + f + " (" + key + ")");
        }

        //try to get object firstly
        if (!key.isEmpty()) {
            try {
                ObjectMetadata fileMetadata = client.getObjectMetadata(bucket, key);
                statistics.incrementReadOps(1);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found exact file: normal file");
                }
                return new OSSFileStatus(fileMetadata.getContentLength(),
                        dateToLong(fileMetadata.getLastModified()),
                        f.makeQualified(uri, workingDir),
                        getDefaultBlockSize(f.makeQualified(uri, workingDir)));
            } catch (OSSException e) {
                if (!e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                    LOG.error(e.getMessage());
                    throw e;
                }
            } catch (ClientException e) {
                LOG.error(e.getMessage());
                throw e;
            }
        }

        //try to append '/' and get empty directory
        if (!key.isEmpty() && !key.endsWith("/")) {
            try {
                String newKey = key + "/";
                ObjectMetadata meta = client.getObjectMetadata(bucket, newKey);
                statistics.incrementReadOps(1);

                if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found file (with /): fake directory");
                    }
                    return new OSSFileStatus(true, true, f.makeQualified(uri, workingDir));
                }
            } catch (OSSException e) {
                if (!e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                    LOG.error(e.getMessage());
                    throw e;
                }
            } catch (ClientException e) {
                LOG.error(e.getMessage());
                throw e;
            }
        }

        //try to append '/' and get non-empty directory
        try {
            if (!key.isEmpty() && !key.endsWith("/")) {
                key = key + "/";
            }
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(key);
            request.setDelimiter("/");
            request.setMaxKeys(1);

            ObjectListing objects = client.listObjects(request);
            statistics.incrementReadOps(1);

            if (!objects.getCommonPrefixes().isEmpty()
                    || objects.getObjectSummaries().size() > 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found path as directory (with /): " +
                            objects.getCommonPrefixes().size() + "/" +
                            objects.getObjectSummaries().size());

                    for (OSSObjectSummary summary : objects.getObjectSummaries()) {
                        LOG.debug("Summary: " + summary.getKey() + " " + summary.getSize());
                    }
                    for (String prefix : objects.getCommonPrefixes()) {
                        LOG.debug("Prefix: " + prefix);
                    }
                }
                return new OSSFileStatus(true, false, f.makeQualified(uri, workingDir));
            } else if (key.isEmpty()) {
                LOG.debug("Found root directory");
                return new OSSFileStatus(true, true, f.makeQualified(uri, workingDir));
            }
        } catch (OSSException e) {
            if (!e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                LOG.error(e.getMessage());
                throw e;
            }
        } catch (ClientException e) {
            LOG.error(e.getMessage());
            throw e;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Not Found: " + f);
        }
        throw new FileNotFoundException("No such file or directory: " + f);

    }

    /**
     * The src file is on the local disk.  Add it to FS at
     * the given dst name.
     * <p/>
     * This version doesn't need to create a temporary file to calculate the md5.
     * Sadly this doesn't seem to be used by the shell cp :(
     * <p/>
     * delSrc indicates if the source should be removed
     *
     * @param delSrc    whether to delete the src
     * @param overwrite whether to overwrite an existing file
     * @param src       path
     * @param dst       path
     */
    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
                                  Path dst) throws IOException {
        String key = pathToKey(dst);

        if (!overwrite && exists(dst)) {
            throw new IOException(dst + " already exists");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Copying local file from " + src + " to " + dst);
        }

        // Since we have a local file, we don't need to stream into a temporary file
        LocalFileSystem local = getLocal(getConf());
        File srcfile = local.pathToFile(src);

        final ObjectMetadata om = new ObjectMetadata();
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, srcfile);
        putObjectRequest.setMetadata(om);
        putObjectRequest.setProgressListener(new ProgressListener() {
            public void progressChanged(ProgressEvent progressEvent) {
                switch (progressEvent.getEventType()) {
                    case TRANSFER_PART_COMPLETED_EVENT:
                        statistics.incrementWriteOps(1);
                        break;
                    default:
                        break;
                }
            }
        });

        try {
            client.putObject(putObjectRequest);
            statistics.incrementWriteOps(1);
        } catch (OSSException | ClientException e) {
            throw new IOException("Got interrupted, cancelling");
        }
        // This will delete unnecessary fake parent directories
        finishedWrite(key);
        if (delSrc) {
            local.delete(src, false);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            // Shutdown the instance to release any allocated resources
            if (client != null) {
                client.shutdown();
                client = null;
            }
        }
    }

    /**
     * Override getCanonicalServiceName because we don't support token in OSS
     */
    @Override
    public String getCanonicalServiceName() {
        // Does not support Token
        return null;
    }

    private void copyFile(String srcKey, String dstKey) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("copyFile " + srcKey + " -> " + dstKey);
        }

        ObjectMetadata om = client.getObjectMetadata(bucket, srcKey);
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
        copyObjectRequest.setNewObjectMetadata(om);
        copyObjectRequest.setProgressListener(new ProgressListener() {
            public void progressChanged(ProgressEvent progressEvent) {
                switch (progressEvent.getEventType()) {
                    case TRANSFER_PART_COMPLETED_EVENT:
                        statistics.incrementWriteOps(1);
                        break;
                    default:
                        break;
                }
            }
        });
        try {
            client.copyObject(copyObjectRequest);
            statistics.incrementWriteOps(1);
        } catch (OSSException | ClientException e) {
            throw new IOException("Got interrupted, cancelling");
        }
    }

    public void finishedWrite(String key) throws IOException {
        deleteUnnecessaryEmptyDirectories(keyToPath(key).getParent());
    }

    private void deleteUnnecessaryEmptyDirectories(Path f) throws IOException {
        while (true) {
            try {
                String key = pathToKey(f);
                if (key.isEmpty()) {
                    break;
                }

                OSSFileStatus status = getFileStatus(f);

                if (status.isDirectory() && status.isEmptyDirectory()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Deleting fake directory " + key + "/");
                    }
                    client.deleteObject(bucket, key + "/");
                    statistics.incrementWriteOps(1);
                }
            } catch (FileNotFoundException | OSSException e) {
                throw new IOException("Got interrupted, cancelling");
            }

            if (f.isRoot()) {
                break;
            }

            f = f.getParent();
        }
    }

    private void createEmptyDirectory(final String bucketName, final String objectName)
            throws ClientException, OSSException {
        if (!objectName.endsWith("/")) {
            createEmptyObject(bucketName, objectName + "/");
        } else {
            createEmptyObject(bucketName, objectName);
        }
    }

    private void createEmptyObject(final String bucketName, final String objectName)
            throws ClientException, OSSException {

        final InputStream nullStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return -1;
            }
        };

        final ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0L);

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, nullStream, om);
        client.putObject(putObjectRequest);
        statistics.incrementWriteOps(1);
    }

    /**
     * Return the number of bytes that large input files should be optimally
     * be split into to minimize i/o time.
     *
     * @deprecated use {@link #getDefaultBlockSize(Path)} instead
     */
    @Deprecated
    public long getDefaultBlockSize() {
        // default to 32MB: large enough to minimize the impact of seeks
        return OSS_DEFAULT_BLOCK_SIZE;
    }

    private boolean objectRepresentsDirectory(final String name, final long size) {
        return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
    }

    // Handles null Dates that can be returned by OSS
    private long dateToLong(final Date date) {
        if (date == null) {
            return 0L;
        }
        return date.getTime();
    }

}
