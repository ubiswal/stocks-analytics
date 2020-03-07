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
            System.out.println(String.format("Did not find any directory with current date %s", currentDate));
            return;
        }
        for (String symbol : stockSymbols) {
            try {
                StockPrices stockObj = convertS3JsonToClass(symbol, currentDate, hour);
                //this is for debug only
                System.out.println(String.format("Symbol = %s",symbol));
                List <Float> highPricesList = new ArrayList<>();
                for (Map.Entry<String, TimeSeriesEntry> entry: stockObj.getTimeSeriesEntries().entrySet()) {
                    highPricesList.add(entry.getValue().getHigh());
                }
                Float maxPrice = Collections.max(highPricesList);
                saveStockAnalyticToDynamo(maxPrice, symbol);
            } catch (IOException | SdkClientException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveStockAnalyticToDynamo(Float maxprice, String symbol){
        PutItemRequest request = new PutItemRequest();
        request.setTableName("Analytics-testing");

        Map<String, AttributeValue> map = new HashMap<>();
        map.put("symb", new AttributeValue().withS(symbol));
        map.put("type", new AttributeValue().withS("1_maxprice"));
        map.put("value", new AttributeValue().withN(Float.toString(maxprice)));
        request.setItem(map);
        try {
            PutItemResult result = dynamoDBClient.putItem(request);
            System.out.println(String.format("Saved the high price %f for stock %s",maxprice, symbol));
        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }

    }

}



