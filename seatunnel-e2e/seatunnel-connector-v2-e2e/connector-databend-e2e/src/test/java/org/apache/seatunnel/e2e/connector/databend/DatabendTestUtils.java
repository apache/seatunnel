package org.apache.seatunnel.e2e.connector.databend;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabendTestUtils {
    /**
     * using AWS SDK create MinIO bucket
     *
     * @param bucketName bucket
     * @return success or not
     */
    public static boolean createMinIOBucketWithAWSSDK(String bucketName) {
        try {
            log.info("using AWS SDK to create MinIO bucket: {}", bucketName);

            AwsClientBuilder.EndpointConfiguration endpointConfig =
                    new AwsClientBuilder.EndpointConfiguration(
                            "http://localhost:9000", "us-east-1");

            AWSCredentials credentials = new BasicAWSCredentials("minioadmin", "minioadmin");
            AWSCredentialsProvider credentialsProvider =
                    new AWSStaticCredentialsProvider(credentials);

            AmazonS3 s3Client =
                    AmazonS3ClientBuilder.standard()
                            .withEndpointConfiguration(endpointConfig)
                            .withCredentials(credentialsProvider)
                            .withPathStyleAccessEnabled(true)
                            .disableChunkedEncoding()
                            .build();

            boolean bucketExists = s3Client.doesBucketExistV2(bucketName);
            if (bucketExists) {
                log.info("bucket {} exist，no need to create", bucketName);
                return true;
            }

            s3Client.createBucket(bucketName);
            log.info("create MinIO bucket success: {}", bucketName);
            return true;
        } catch (Exception e) {
            log.error("using AWS SDK to create MinIO failed", e);
            return false;
        }
    }
}
