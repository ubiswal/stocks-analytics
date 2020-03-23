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
        NewsAnalyzer newsAnalyzer = new NewsAnalyzer(s3Client, dynamoDB, stockSymbols, "stocks-testing");
        AnalyzerCron analyzerCron = new AnalyzerCron(analyzer, newsAnalyzer);

        Timer timer = new Timer();
        timer.schedule(analyzerCron, 0, 3600000);
    }
}
