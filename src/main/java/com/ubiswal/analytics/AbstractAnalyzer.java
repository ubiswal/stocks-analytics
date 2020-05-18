package com.ubiswal.analytics;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

abstract public class AbstractAnalyzer {
    protected final String bucketName;
    protected final AmazonS3 s3Client;
    protected final String analysisDate;
    protected final String analysisHour;

    public AbstractAnalyzer(AmazonS3 s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.analysisDate = todaysDate();
        this.analysisHour = Integer.toString(findLatestHourInBucket());
    }

    protected String todaysDate() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
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
