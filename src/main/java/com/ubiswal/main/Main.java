package com.ubiswal.main;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.logging.FileHandler;
import com.twitter.logging.LoggerFactory;
import com.ubiswal.analytics.NewsAnalyzer;
import com.ubiswal.analytics.StocksAnalyzer;
import com.twitter.logging.Logger;
import com.twitter.logging.Handler;
import com.ubiswal.config.Config;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

class AnalyzerCron extends TimerTask{
    private final StocksAnalyzer stocksAnalyzer;
    private final NewsAnalyzer newsAnalyzer;

    AnalyzerCron(StocksAnalyzer stocksAnalyzer, NewsAnalyzer newsAnalyzer){
        this.stocksAnalyzer = stocksAnalyzer;
        this.newsAnalyzer = newsAnalyzer;
    }

    @Override
    public void run() {
        stocksAnalyzer.runAnalyzer();
        newsAnalyzer.runAnalyzer();
    }
}
public class Main {
    private static final String BUCKETNAME = "stocks-testing" ;

    public static void main(String args[]) throws IOException {
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        Config configObj = getConfig(s3Client);
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        StocksAnalyzer analyzer = new StocksAnalyzer(s3Client, dynamoDB, configObj.getStockSymbols(), "stocks-testing");
        NewsAnalyzer newsAnalyzer = new NewsAnalyzer(s3Client, dynamoDB, configObj.getStockSymbols(), "stocks-testing");
        AnalyzerCron analyzerCron = new AnalyzerCron(analyzer, newsAnalyzer);

        Timer timer = new Timer();
        timer.schedule(analyzerCron, 0, 3600000);
    }

    private static Config getConfig(final AmazonS3 s3Client) throws IOException {
        try {
            S3Object s3obj = s3Client.getObject(BUCKETNAME , "config.json");
            S3ObjectInputStream inputStream = s3obj.getObjectContent();
            FileUtils.copyInputStreamToFile(inputStream, new File("config.json"));
        } catch (SdkClientException e) {
            System.out.println("Failed to download config file from s3 because " + e.getMessage());
            throw e;
        } catch (IOException e) {
            System.out.println("Failed to save downloaded config file from s3 because " + e.getMessage());
            throw e;
        }

        //use jackson for json to class conversion for the Config
        ObjectMapper mapper = new ObjectMapper();
        try {
            // JSON file to Java object
            Config config = mapper.readValue(new File("config.json"), Config.class);
            return config;
        } catch (IOException e) {
            System.out.println("Failed to read the downloaded config file from s3 because " + e.getMessage());
            throw e;
        }

    }
}
