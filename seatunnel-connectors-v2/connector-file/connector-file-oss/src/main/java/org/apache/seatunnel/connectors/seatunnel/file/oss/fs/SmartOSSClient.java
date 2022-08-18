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

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>Wrapper for OSSClient and provide large file optimized transfer(multiple-part-upload and multiple-part-copy).</p>
 * <b>Only used in OSS File System implementation.</b>
 */
public class SmartOSSClient extends OSSClient {

    private SmartOSSClientConfig configuration;
    private ExecutorService threadPoolExecutor;
    public static final Logger LOG = LoggerFactory.getLogger(OSSClient.class);

    public SmartOSSClient(String endpoint, String accessKeyId, String secretAccessKey,
                          SmartOSSClientConfig config) {
        super(endpoint, accessKeyId, secretAccessKey, config);
        this.configuration = config;
        threadPoolExecutor = new ThreadPoolExecutor(SmartOSSClientConfig.DEFAULT_MAX_POOL_SIZE,
                SmartOSSClientConfig.DEFAULT_MAX_POOL_SIZE, SmartOSSClientConfig.DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public void shutdown() {
        this.threadPoolExecutor.shutdown();
        super.shutdown();
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
            throws OSSException, ClientException {

        ObjectMetadata metadata = this.getObjectMetadata(copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey());
        if (!isMultipartCopy(metadata)) {
            return super.copyObject(copyObjectRequest); //simple copy
        } else {
            return doMultiPartCopy(copyObjectRequest, metadata);  //multi-part copy
        }
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws OSSException, ClientException {
        if (putObjectRequest.getInputStream() != null) {
            return super.putObject(putObjectRequest);
        } else if (putObjectRequest.getFile() != null) {
            File backupFile = putObjectRequest.getFile();
            if (!isMultipartUpload(backupFile)) {
                return super.putObject(putObjectRequest); ////simple upload
            } else {
                return this.doMultiPartUpload(putObjectRequest); //multi-part upload
            }
        }
        return super.putObject(putObjectRequest);
    }

    private CopyObjectResult doMultiPartCopy(CopyObjectRequest copyObjectRequest, ObjectMetadata metadata) {

        //Claim a new upload id for your target bucket
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());
        InitiateMultipartUploadResult initiateMultipartUploadResult = this.initiateMultipartUpload(initiateMultipartUploadRequest);
        String uploadId = initiateMultipartUploadResult.getUploadId();

        // Calculate how many parts to be divided
        long optimalPartSize = calculateOptimalPartSizeForCopy(configuration, metadata.getContentLength());
        long objectSize = metadata.getContentLength();
        int totalParts = (int) Math.ceil((double) objectSize / optimalPartSize);

        // Upload multi-parts by copy mode
        LOG.info("Begin to upload multi parts by copy mode to OSS");
        List<Future<PartETag>> futures = new ArrayList<Future<PartETag>>();
        for (int i = 0; i < totalParts; i++) {
            long startPos = i * optimalPartSize;
            long curPartSize = (i + 1 == totalParts) ? (objectSize - startPos) : optimalPartSize;

            // Construct UploadPartCopyRequest
            UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest(
                    copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey(), copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());
            uploadPartCopyRequest.setUploadId(uploadId);
            uploadPartCopyRequest.setPartSize(curPartSize);
            uploadPartCopyRequest.setBeginIndex(startPos);
            uploadPartCopyRequest.setPartNumber(i + 1);
            futures.add(threadPoolExecutor.submit(new PartCopyCallable(uploadPartCopyRequest)));
        }

        List<PartETag> partETags = collectPartETags(futures);
        //Verify whether all parts are finished
        if (partETags.size() != totalParts) {
            throw new IllegalStateException("Upload multi-parts fail due to some parts are not finished yet");
        } else {
            LOG.info("Succeed to complete multi-parts copy from object named {} to {}", copyObjectRequest.getSourceKey(), copyObjectRequest.getDestinationKey());
        }

        //Complete to upload multi-parts
        LOG.info("Completing to upload multi parts");
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey(), uploadId, partETags);
        CompleteMultipartUploadResult completeMultipartUploadResult = this.completeMultipartUpload(completeMultipartUploadRequest);
        CopyObjectResult result = new CopyObjectResult();
        result.setEtag(completeMultipartUploadResult.getETag());
        return result;
    }

    private PutObjectResult doMultiPartUpload(PutObjectRequest putObjectRequest) {

        File backupFile = putObjectRequest.getFile();

        //Claim a upload id firstly
        String uploadId;
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(putObjectRequest.getBucketName(), putObjectRequest.getKey());
        InitiateMultipartUploadResult result = this.initiateMultipartUpload(request);
        uploadId = result.getUploadId();

        long optimalPartSize = calculateOptimalPartSize(backupFile);
        int totalParts = (int) Math.ceil((double) backupFile.length() / optimalPartSize);

        //Upload multi-parts to the bucket
        if (LOG.isInfoEnabled()) {
            LOG.info("Begin to upload multi-parts to OSS from a {}\n", backupFile.getPath());
        }

        List<Future<PartETag>> futures = new ArrayList<Future<PartETag>>();
        for (int i = 0; i < totalParts; i++) {
            long startPos = i * optimalPartSize;
            long curPartSize = (i + 1 == totalParts) ? (backupFile.length() - startPos) : optimalPartSize;

            futures.add(threadPoolExecutor.submit(new PartUploadCallable(putObjectRequest.getBucketName(), putObjectRequest.getKey(),
                    backupFile, startPos, curPartSize, i + 1, uploadId)));
        }

        List<PartETag> partETags = collectPartETags(futures);

        //Verify whether all parts are finished
        if (partETags.size() != totalParts) {
            throw new IllegalStateException("Upload multi-parts fail due to some parts are not finished yet");
        } else {
            LOG.info("Succeed to complete multi-parts into an object named {}", putObjectRequest.getKey());
        }

        // Make part numbers in ascending order
        Collections.sort(partETags, new Comparator<PartETag>() {
            @Override
            public int compare(PartETag p1, PartETag p2) {
                return p1.getPartNumber() - p2.getPartNumber();
            }
        });

        LOG.info("Completing to upload multi-parts\n");
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(putObjectRequest.getBucketName(), putObjectRequest.getKey(), uploadId, partETags);
        CompleteMultipartUploadResult completeResult = this.completeMultipartUpload(completeMultipartUploadRequest);
        PutObjectResult putObjectResult = new PutObjectResult();
        putObjectResult.setETag(completeResult.getETag());
        return putObjectResult;

    }

    private boolean isMultipartUpload(File file) {
        long contentLength = file.length();
        return contentLength > configuration.getMultipartUploadThreshold();
    }

    private boolean isMultipartCopy(ObjectMetadata metadata) {
        return metadata.getContentLength() > configuration.getMultipartCopyThreshold();
    }

    //Returns the optimal part size, in bytes, for each individual part upload in a multipart upload.
    private long calculateOptimalPartSize(File file) {
        double contentLength = file.length();
        double optimalPartSize = (double) contentLength / (double) SmartOSSClientConfig.MAXIMUM_UPLOAD_PARTS;
        // round up so we don't push the upload over the maximum number of parts
        optimalPartSize = Math.ceil(optimalPartSize);
        return (long) Math.max(optimalPartSize, configuration.getMinimumUploadPartSize());
    }

    //Calculates the optimal part size of each part request if the copy operation is carried out as multi-part copy.
    public static long calculateOptimalPartSizeForCopy(SmartOSSClientConfig configuration, long contentLengthOfSource) {
        double optimalPartSize = (double) contentLengthOfSource / (double) SmartOSSClientConfig.MAXIMUM_UPLOAD_PARTS;
        // round up so we don't push the copy over the maximum number of parts
        optimalPartSize = Math.ceil(optimalPartSize);
        return (long) Math.max(optimalPartSize, configuration.getMultipartCopyPartSize());
    }

    private List<PartETag> collectPartETags(List<Future<PartETag>> futures) {

        final List<PartETag> partETags = new ArrayList<PartETag>();
        for (Future<PartETag> future : futures) {
            try {
                partETags.add(future.get());
            } catch (Exception e) {
                throw new ClientException(
                        "Unable to complete multi-part upload. Individual part upload failed : "
                                + e.getCause().getMessage(), e.getCause());
            }
        }
        return partETags;
    }

    class PartCopyCallable implements Callable<PartETag> {

        private UploadPartCopyRequest uploadPartCopyRequest;

        public PartCopyCallable(UploadPartCopyRequest uploadPartCopyRequest) {
            this.uploadPartCopyRequest = uploadPartCopyRequest;
        }

        @Override
        public PartETag call() throws Exception {
            UploadPartCopyResult uploadPartCopyResult = SmartOSSClient.this.uploadPartCopy(uploadPartCopyRequest);
            LOG.info("\tPart#" + uploadPartCopyResult.getPartNumber() + " done");
            return uploadPartCopyResult.getPartETag();
        }
    }

    class PartUploadCallable implements Callable<PartETag> {
        private File localFile;
        private long startPos;
        private long partSize;
        private int partNumber;
        private String uploadId;
        private String bucketName;
        private String key;

        public PartUploadCallable(String bucketName, String key, File localFile, long startPos, long partSize, int partNumber, String uploadId) {
            this.localFile = localFile;
            this.startPos = startPos;
            this.partSize = partSize;
            this.partNumber = partNumber;
            this.uploadId = uploadId;
            this.bucketName = bucketName;
            this.key = key;
        }

        @Override
        public PartETag call() {
            InputStream inputStream = null;
            UploadPartResult uploadPartResult = null;
            try {
                inputStream = new FileInputStream(this.localFile);
                inputStream.skip(this.startPos);

                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucketName);
                uploadPartRequest.setKey(key);
                uploadPartRequest.setUploadId(this.uploadId);
                uploadPartRequest.setInputStream(inputStream);
                uploadPartRequest.setPartSize(this.partSize);
                uploadPartRequest.setPartNumber(this.partNumber);

                uploadPartResult = SmartOSSClient.this.uploadPart(uploadPartRequest);
                LOG.info("Key {} Part# {} is done", this.key, this.partNumber);

            } catch (Exception e) {
                throw new ClientException(e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        throw new ClientException(e);
                    }
                }
            }
            return uploadPartResult.getPartETag();
        }
    }

}



