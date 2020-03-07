package com.ubiswal.analytics;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.text.SimpleDateFormat;
import java.util.Date;

abstract public class AbstractAnalyzer {
    protected String bucketName;
    protected AmazonS3 s3Client;

    public AbstractAnalyzer(AmazonS3 s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    protected String todaysDate() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        String currentDate = formatter.format(date);
        return currentDate;
    }

    protected int findLatestHourInBucket() {
        int maxHour = -1;
        String currentDate = todaysDate();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(currentDate);
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
            String[] folderName = objectSummary.getKey().split("/");
            int hour = new Integer(folderName[1]);
            if (hour > maxHour) {
                maxHour = hour;
            }
        }
        return maxHour;
    }

    public abstract void runAnalyzer();

}
