package com.ubiswal.analytics;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;
import com.twitter.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
class TimeSeriesEntry {
    @Getter
    @Setter
    @JsonProperty("1. open")
    private float open;
    @Getter
    @Setter
    @JsonProperty("2. high")
    private float high;
    @Getter
    @Setter
    @JsonProperty("3. low")
    private float low;
    @Getter
    @Setter
    @JsonProperty("4. close")
    private float close;
    @Getter
    @Setter
    @JsonProperty("5. volume")
    private int volume;

}

@JsonIgnoreProperties(ignoreUnknown = true)
class StockPrices {
    //private String symbol;
    @Getter
    @Setter
    @JsonProperty("Time Series (5min)")
    private Map<String, TimeSeriesEntry> timeSeriesEntries;
}


public class StocksAnalyzer extends AbstractAnalyzer {

    private AmazonS3 s3Client;
    private AmazonDynamoDB dynamoDBClient;
    private List<String> stockSymbols;
    private String bucketName;
    private Logger log = Logger.get(this.getClass());

    public StocksAnalyzer(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, List<String> stockSymbols, String bucketName) {
        super(s3Client, bucketName);
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.stockSymbols = stockSymbols;
        this.bucketName = bucketName;
    }


    private StockPrices convertS3JsonToClass(String symbol, String currentDate, String hour) throws IOException {
        S3Object s3obj = s3Client.getObject(bucketName, String.format("%s/%s/%s/stock.json", currentDate, hour, symbol));
        S3ObjectInputStream inputStream = s3obj.getObjectContent();
        FileUtils.copyInputStreamToFile(inputStream, new File("stock.json"));

        //use jackson for json to class conversion
        ObjectMapper mapper = new ObjectMapper();
        // JSON file to Java object
        StockPrices stockPriceObj = mapper.readValue(new File("stock.json"), StockPrices.class);
        return stockPriceObj;
    }


    //convert all stocks json to Objects
    @Override
    public void runAnalyzer() {
        String currentDate = todaysDate();
        String hour = Integer.toString(findLatestHourInBucket());
        if (hour.equals("-1")){
            log.warning(String.format("Did not find any directory with current date %s", currentDate));
            return;
        }
        for (String symbol : stockSymbols) {
            try {
                log.info(String.format("Running stocks analyzer for symbol %s",symbol));
                StockPrices stockObj = convertS3JsonToClass(symbol, currentDate, hour);
                calcMaxPrice(symbol, stockObj);
                bestTimeForProfitSell(symbol, stockObj);
            } catch (IOException | SdkClientException e) {
                e.printStackTrace();
            }
        }
    }

    private void calcMaxPrice(String symbol, StockPrices stockObj){
        log.info(String.format("About to calculate max price for Stock Symbol = %s",symbol));
        List <Float> highPricesList = new ArrayList<>();
        for (Map.Entry<String, TimeSeriesEntry> entry: stockObj.getTimeSeriesEntries().entrySet()) {
            highPricesList.add(entry.getValue().getHigh());
        }
        Float maxPrice = Collections.max(highPricesList);
        saveStockAnalyticToDynamo("1_maxprice", Float.toString(maxPrice), symbol);
    }

    private void bestTimeForProfitSell(String symbol, StockPrices stockObj){
        class TimeAndPriceOfStock{
            public String timestamp;
            public float stockValue;
            TimeAndPriceOfStock(String timestamp, float stockValue){
                this.timestamp = timestamp;
                this.stockValue = stockValue;
            }
        }

        TreeMap<String, TimeSeriesEntry> sortedEntries = new TreeMap<>(stockObj.getTimeSeriesEntries());
        List<TimeAndPriceOfStock> averageStockPrice = new ArrayList<>();
        for (Map.Entry<String, TimeSeriesEntry> entry: sortedEntries.entrySet()) {
            float averageStockPricePerInterval = (entry.getValue().getOpen() + entry.getValue().getClose() +entry.getValue().getHigh() + entry.getValue().getLow())/4;
            averageStockPrice.add(new TimeAndPriceOfStock(entry.getKey(), averageStockPricePerInterval));
        }
        float maxProfit = 0;
        String maxBuyIdx = "";
        String maxSellIdx = "";
        for (int i = 1; i < averageStockPrice.size(); i++){
            for (int j = 0; j < i; j++ ){
                if (maxProfit < averageStockPrice.get(i).stockValue-averageStockPrice.get(j).stockValue){
                    maxProfit = averageStockPrice.get(i).stockValue-averageStockPrice.get(j).stockValue;
                    maxBuyIdx = averageStockPrice.get(j).timestamp;
                    maxSellIdx = averageStockPrice.get(i).timestamp;
                }
            }
        }
        if (maxProfit > 0){
            String result = String.format("A best profit of %f could have been achieved for stock %s if bought at  %s and sold at %s", maxProfit, symbol, maxBuyIdx, maxSellIdx);
            saveStockAnalyticToDynamo("2_bestProfit", result, symbol);
            log.info(result);
        }
        else{
            String result = String.format("Could not sell stock %s with any profits today!", symbol);
            saveStockAnalyticToDynamo("2_bestProfit", result, symbol);
            log.info(result);
        }


    }

    private void saveStockAnalyticToDynamo(String analyticType, String analyticValue, String symbol){
        PutItemRequest request = new PutItemRequest();
        request.setTableName("Analytics-testing");

        Map<String, AttributeValue> map = new HashMap<>();
        map.put("symb", new AttributeValue().withS(symbol));
        map.put("type", new AttributeValue().withS(analyticType));
        map.put("value", new AttributeValue().withS(analyticValue));
        request.setItem(map);
        try {
            PutItemResult result = dynamoDBClient.putItem(request);
        } catch (AmazonServiceException e) {

            log.error(e.getErrorMessage());

        }

    }

}



