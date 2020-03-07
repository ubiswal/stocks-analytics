package com.ubiswal.main;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.ubiswal.analytics.NewsAnalyzer;
import com.ubiswal.analytics.StocksAnalyzer;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String args[]){
        final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        List<String> stockSymbols = new ArrayList<>();
        stockSymbols.add("MSFT");
        stockSymbols.add("AAPL");
        StocksAnalyzer analyzer = new StocksAnalyzer(s3Client, dynamoDB, stockSymbols, "stocks-testing");
        analyzer.runAnalyzer();
        NewsAnalyzer newsAnalyzer = new NewsAnalyzer(s3Client, dynamoDB, stockSymbols, "stocks-testing");
        newsAnalyzer.runAnalyzer();
    }
}
