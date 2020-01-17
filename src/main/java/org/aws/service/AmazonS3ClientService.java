package org.aws.service;

import java.io.InputStream;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public interface AmazonS3ClientService {

	void createBucket(String bucketName);
	
	List<String> listBuckets();
	
	ObjectMetadata createEmptyFolderInBucket(String bucketName, String folderName);
	
	ObjectMetadata uploadFileToBucket(String bucketName, String uploadKey, InputStream inputStream);
	
	S3ObjectInputStream downloadFileFromBucket(String bucketName, String key);
}
