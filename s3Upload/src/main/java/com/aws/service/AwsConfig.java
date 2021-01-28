package com.aws.worker.rest.config;

import static com.amazonaws.services.s3.internal.Constants.MB;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

/**
 * The Class AwsConfig.
 */
@Component
public class AwsConfig {

	@Value("${aws.access_key_id}")
	private String awsId;

	@Value("${aws.secret_access_key}")
	private String awsKey;

	@Value("${s3.bucket}")
	private String bucketName;

	private static AmazonS3 s3Client;

	private static final int MAX_CONNECTIONS = 100;

	private static final int MAX_ERROR_RETRY = 10;

	
	/**
	 * @param clientId
	 * @param secretKey
	 * @return
	 */
	public AmazonS3 s3client(String clientId, String secretKey) {

		ClientConfiguration config = new ClientConfiguration();
		config.setMaxConnections(MAX_CONNECTIONS);
		config.setMaxErrorRetry(MAX_ERROR_RETRY);
		config.setRetryPolicy(new RetryPolicy(null, null, MAX_ERROR_RETRY, true));

		BasicAWSCredentials awsCreds = new BasicAWSCredentials(clientId, secretKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName("ap-southeast-1"))
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withClientConfiguration(config).build();

		setS3Client(s3Client);
		return s3Client;
	}

	public AmazonS3 s3client() {

		ClientConfiguration config = new ClientConfiguration();
		config.setMaxConnections(MAX_CONNECTIONS);
		config.setMaxErrorRetry(MAX_ERROR_RETRY);
		config.setRetryPolicy(new RetryPolicy(null, null, MAX_ERROR_RETRY, true));

		BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsId, awsKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName("ap-southeast-1"))
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withClientConfiguration(config).build();

		setS3Client(s3Client);
		return s3Client;
	}

	/**
	 * Gets the s 3 client.
	 *
	 * @return the s 3 client
	 */
	public static AmazonS3 gets3Client() {
		return s3Client;
	}

	/**
	 * Sets the s 3 client.
	 *
	 * @param s3Client the new s 3 client
	 */
	public static void setS3Client(AmazonS3 s3Client) {
		AwsConfig.s3Client = s3Client;
	}

	/**
	 * Transfer manager.
	 *
	 * @return the transfer manager
	 */
	@Bean
	public TransferManager transferManager() {

		TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3client())
				.withMinimumUploadPartSize(Long.valueOf(5 * MB)).withMultipartUploadThreshold(Long.valueOf(16 * MB))
				.withMultipartCopyPartSize(Long.valueOf(5 * MB)).withMultipartCopyThreshold(Long.valueOf(100 * MB))
				.withExecutorFactory(() -> createExecutorService(10)).build();

		return tm;
	}

	/**
	 * Creates the executor service.
	 *
	 * @param threadNumber the thread number
	 * @return the thread pool executor
	 */
	private ThreadPoolExecutor createExecutorService(int threadNumber) {
		ThreadFactory threadFactory = new ThreadFactory() {
			private int threadCount = 1;

			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName("amazon-s3-transfer-manager-worker-" + threadCount++);
				return thread;
			}
		};
		return (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber, threadFactory);
	}

}
