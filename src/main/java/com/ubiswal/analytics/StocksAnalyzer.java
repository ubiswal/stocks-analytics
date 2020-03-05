package com.ubiswal.analytics;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import  lombok.Setter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

class TimeSeriesEntry{
    @Getter @Setter  private int open;
    @Getter @Setter  private int close;
    @Getter @Setter  private int high;
    @Getter @Setter  private int low;
    @Getter @Setter  private int volume;

}

class StockPrices {
    @Getter @Setter  private String symbol;
    @Getter @Setter  private List<TimeSeriesEntry> timeSeriesEntries;
}


public class StocksAnalyzer {
    private AmazonS3 s3Client;
    private List<String> stockSymbols;
    private String bucketName;
    public StocksAnalyzer(AmazonS3 s3Client, List <String>stockSymbols, String bucketName){
        this.s3Client = s3Client;
        this.stockSymbols = stockSymbols;
        this.bucketName = bucketName;
    }
    private int findLatestHourOfStock(){
        int maxHour = 0;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        String currentDate = formatter.format(date);
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

    private void convertS3JsontoClass(String symbol){
        try {
            S3Object s3obj = s3Client.getObject(bucketName , "");
            S3ObjectInputStream inputStream = s3obj.getObjectContent();
            FileUtils.copyInputStreamToFile(inputStream, new File("config.json"));
        } catch (
                SdkClientException e) {
            System.out.println("Failed to download config file from s3 because " + e.getMessage());
            throw e;
        } catch (IOException e) {
            System.out.println("Failed to save downloaded config file from s3 because " + e.getMessage());
            try {
                throw e;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }


}
