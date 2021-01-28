package com.aws.worker.rest.service.impl;

import java.io.File;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.aws.worker.rest.config.AwsConfig;
import com.aws.worker.rest.service.AwsUploadSnapshotService;
import com.cloud.api.common.util.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class SnapshotServiceImpl.
 */
@Service
@Slf4j
public class AwsUploadSnapshotServiceImpl {

	@Value("${s3.bucket}")
	protected String bucketName;

	@Autowired
	protected TransferManager transferManager;

	@Autowired
	AwsConfig config;

	public void uploadFile(String keyName, String uploadFilePath, Long size) {
		log.info("uploadFile - Start");
	
		if (bucketAvilable() == true) {

			final PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(uploadFilePath));

			log.info("[uploadFile] uploading file {} to aws", keyName);

			float[] buffer = { 0.0f };

			request.setGeneralProgressListener(new ProgressListener() {
				public void progressChanged(ProgressEvent progressEvent) {

					buffer[0] = buffer[0] + progressEvent.getBytesTransferred();

					log.info(" uploading file {} percentage completed >>> {} ", keyName,
							Math.round((buffer[0] / size) * 100));

					if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {

						log.info("AWS Upload completed");
					}
				}
			});
			log.info("[uploadFile] aws uploading initiated for file ");
			Upload upload = transferManager.upload(request);
			try {
				upload.waitForCompletion();
			} catch (Exception e) {
				log.error("uploadFile Exception: ", e);
			}
			log.info("uploadFile : End");

		}
	}

	public void uploadFileInputStream(String keyName, String uploadFilePath, Long size) {
		log.info("uploadFileInputStream - Start");
		final PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(uploadFilePath));

		log.info("[uploadFileInputStream] uploading file {} to aws", keyName);

		float[] buffer = { 0.0f };

		request.setGeneralProgressListener(new ProgressListener() {
			public void progressChanged(ProgressEvent progressEvent) {

				buffer[0] = buffer[0] + progressEvent.getBytesTransferred();

				log.info(" uploading file {} percentage completed >>> {} ", keyName,
						Math.round((buffer[0] / size) * 100));

				if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {

					log.info("AWS Upload completed");
				}
			}
		});
		log.info("[uploadFile] aws uploading initiated for file ");
		log.info("uploadFileInputStream : End");
		Upload upload = transferManager.upload(request);

	}

	public boolean bucketAvilable() {
		log.info("bucketAvilable : Start");
		boolean isContainerExist = false;
		AmazonS3 s3Client = config.s3client();
		log.info("bucketAvilable : checking bucket");
		if (!CollectionUtils.isNullOrEmpty(s3Client.listBuckets())) {
			for (Bucket bucket : s3Client.listBuckets()) {
				if (bucket.getName().equalsIgnoreCase(bucketName)) {
					isContainerExist = true;
				}
			}
		}
		log.info("bucketAvilable : End");
		return isContainerExist;
	}

}
