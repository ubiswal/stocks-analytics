package com.ubiswal.analytics;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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
import org.apache.http.HttpException;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.VectorGraphicsEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYSeries;

import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

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
    private static final String S3_BUCKET_NAME = "ubiswal-website-contents";

    public StocksAnalyzer(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, List<String> stockSymbols, String bucketName) {
        super(s3Client, bucketName);
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.stockSymbols = stockSymbols;
        this.bucketName = bucketName;
    }

    private boolean validateStockPricesForSymbol(StockPrices obj) {
        if (obj.getTimeSeriesEntries() == null) {
            return false;
        } else {
            return true;
        }
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
        String analysisHour = Integer.toString(findLatestHourInBucket());
        if (analysisHour.equals("-1")){
            log.warning(String.format("Did not find any directory with current date %s", analysisDate));
            return;
        } else {
            log.info(String.format("Getting all data for %s -- %s hours", analysisDate, analysisHour));
        }
        for (String symbol : stockSymbols) {
            try {
                log.info(String.format("Running stocks analyzer for symbol %s",symbol));
                StockPrices stockObj = convertS3JsonToClass(symbol, analysisDate, analysisHour);
                if(stockObj == null || stockObj.getTimeSeriesEntries() == null) {
                    log.error(String.format("Failed to fetch stock data for %s. You may be out of call quota. Please check the crawler.", symbol));
                    continue;
                }
                boolean freshness = isDataFresh(symbol, stockObj);

                if(!validateStockPricesForSymbol(stockObj)) {
                    log.warning(String.format("Failed to generate data for symbol %s", symbol));
                    // TODO: Add code to delete any entries for this symbol in dynamo DB
                    continue;
                }
                calcMaxPrice(symbol, stockObj, freshness);
                bestTimeForProfitSell(symbol, stockObj, freshness);
                generateGraphsStockPrices(symbol, stockObj);
                diffInStockPrice(symbol, stockObj, freshness);
            } catch (IOException | SdkClientException | HttpException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isDataFresh(String stockSymbol, StockPrices stockObj) {
        TreeMap<String, TimeSeriesEntry> sortedEntries = new TreeMap<>(stockObj.getTimeSeriesEntries());
        if(sortedEntries.size() > 0) {
            String greatestTimeStamp = sortedEntries.lastEntry().getKey();
            try {
                Calendar calendar = GregorianCalendar.getInstance();
                Date providedDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(greatestTimeStamp);
                calendar.setTime(providedDate);
                int providedDay = calendar.get(Calendar.DAY_OF_MONTH);

                Date currentDate = new Date();
                calendar.setTime(currentDate);
                int currentDay = calendar.get(Calendar.DAY_OF_MONTH);

                if (providedDay < currentDay-1) {
                    log.warning(String.format("Data for %s is stale", stockSymbol));
                    return false;
                } else {
                    log.info(String.format("Data for %s is fresh", stockSymbol));
                    return true;
                }
            } catch (ParseException e) {
                log.error(e, String.format("Could not parse date provided by alpha vantage (%s). Expected format was yyyy-MM-dd hh:mm:ss. Will assume data as fresh"));
                return true;
            }
        }
        return true;
    }

    private void calcMaxPrice(String symbol, StockPrices stockObj, boolean fresh){
        log.info(String.format("About to calculate max price for Stock Symbol = %s",symbol));
        List <Float> highPricesList = new ArrayList<>();
        for (Map.Entry<String, TimeSeriesEntry> entry: stockObj.getTimeSeriesEntries().entrySet()) {
            highPricesList.add(entry.getValue().getHigh());
        }
        Float maxPrice = Collections.max(highPricesList);
        saveStockAnalyticToDynamo("1_maxprice", Float.toString(maxPrice), symbol, fresh? "fresh" : "stale");
    }

    private void bestTimeForProfitSell(String symbol, StockPrices stockObj, boolean fresh){
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
        if (maxProfit > 0) {
            String result = String.format("%s;%s;%s", maxProfit, maxBuyIdx, maxSellIdx);
            saveStockAnalyticToDynamo("2_bestProfit", result, symbol, fresh? "fresh" : "stale");
            log.info(result);
        }
        else{
            String result = "0:-1:-1";
            saveStockAnalyticToDynamo("2_bestProfit", result, symbol, fresh? "fresh" : "stale");
            log.info(result);
        }


    }

    private void saveStockAnalyticToDynamo(String analyticType, String analyticValue, String symbol, String status){
        PutItemRequest request = new PutItemRequest();
        request.setTableName("Analytics-testing");

        Map<String, AttributeValue> map = new HashMap<>();
        map.put("symb", new AttributeValue().withS(symbol));
        map.put("type", new AttributeValue().withS(analyticType));
        map.put("value", new AttributeValue().withS(analyticValue));
        map.put("status", new AttributeValue().withS(status));
        request.setItem(map);
        try {
            PutItemResult result = dynamoDBClient.putItem(request);
        } catch (AmazonServiceException e) {
            log.error(e.getErrorMessage());
        }

    }

    private void uploadToS3(String s3KeyName, String localFileName) throws HttpException {
        try {
            FileInputStream stream = new FileInputStream(localFileName);
            PutObjectRequest request = new PutObjectRequest(S3_BUCKET_NAME, s3KeyName, stream, null).withCannedAcl(CannedAccessControlList.PublicRead);
            s3Client.putObject(request);
        } catch (SdkClientException | FileNotFoundException e) {
            throw new HttpException("Failed to upload to s3 because " + e.getMessage());
        }
    }

    private void diffInStockPrice(String symbol, StockPrices stockObj, boolean fresh) {
        log.info(String.format("Checking the diff in stock prices for %s", symbol));
        TreeMap<String, TimeSeriesEntry> sortedEntries = new TreeMap<>(stockObj.getTimeSeriesEntries());
        float start = sortedEntries.firstEntry().getValue().getOpen();
        float end = sortedEntries.lastEntry().getValue().getClose();
        saveStockAnalyticToDynamo("3_diff", Float.toString(end-start), symbol, fresh? "fresh" : "stale");
    }

    private void generateGraphsStockPrices(String symbol, StockPrices stockObj) throws IOException, HttpException {
        TreeMap<String, TimeSeriesEntry> sortedEntries = new TreeMap<>(stockObj.getTimeSeriesEntries());
        List<Float> prices = new ArrayList<>();
        for (Map.Entry<String, TimeSeriesEntry> ts : sortedEntries.entrySet()) {
            prices.add(ts.getValue().getHigh());
        }

        XYChart chart1; //smaller size graph
        XYChart chart2; //larger size graph

        chart1 = new XYChart(300, 300);
        chart2 = new XYChart(800, 300);

        XYSeries series1 = chart1.addSeries(String.format("Stock prices for %s", symbol), null, prices);
        chart1.getStyler().setMarkerSize(0);
        chart1.getStyler().setLegendVisible(false);
        chart1.getStyler().setPlotGridLinesVisible(false);
        chart1.getStyler().setPlotBorderVisible(false);
        chart1.getStyler().setXAxisTicksVisible(false);

        XYSeries series2 = chart2.addSeries(String.format("Stock prices for %s", symbol), null, prices);
        chart2.getStyler().setMarkerSize(0);
        chart2.getStyler().setLegendVisible(false);
        chart2.getStyler().setPlotGridLinesVisible(false);
        chart2.getStyler().setPlotBorderVisible(false);
        chart2.getStyler().setXAxisTicksVisible(false);

        // Dark graph
        //Dark and smaller size graph
        chart1.getStyler().setChartBackgroundColor(new Color(0x28, 0x28, 0x28));
        series1.setFillColor(new Color(28, 28, 28));
        series1.setLineColor(new Color(0x2E, 0xA2, 0x31));
        chart1.getStyler().setPlotBackgroundColor(new Color(0x28, 0x28, 0x28));
        chart1.getStyler().setAxisTickLabelsColor(new Color(0x2E, 0xA2, 0x31));

        BitmapEncoder.saveBitmap(chart1, String.format("./%s_dark_small.jpg", symbol), BitmapEncoder.BitmapFormat.JPG);
        String s3KeyNameDarkSmall = String.format("%s_dark_small.jpg", symbol);
        uploadToS3(s3KeyNameDarkSmall, String.format("%s_dark_small.jpg", symbol));

        //Dark and larger size graph
        chart2.getStyler().setChartBackgroundColor(new Color(0x28, 0x28, 0x28));
        series2.setFillColor(new Color(28, 28, 28));
        series2.setLineColor(new Color(0x2E, 0xA2, 0x31));
        chart2.getStyler().setPlotBackgroundColor(new Color(0x28, 0x28, 0x28));
        chart2.getStyler().setAxisTickLabelsColor(new Color(0x2E, 0xA2, 0x31));

        BitmapEncoder.saveBitmap(chart2, String.format("./%s_dark_large.jpg", symbol), BitmapEncoder.BitmapFormat.JPG);
        String s3KeyNameDarkLarge = String.format("%s_dark_large.jpg", symbol);
        uploadToS3(s3KeyNameDarkLarge, String.format("%s_dark_large.jpg", symbol));

        // Light graph
        chart1.getStyler().setChartBackgroundColor(new Color(0xFF, 0xFF, 0xFF));
        series1.setFillColor(new Color(0xFF, 0xFF, 0xFF));
        series1.setLineColor(new Color(0xFF, 0, 0));
        chart1.getStyler().setPlotBackgroundColor(new Color(0xFF, 0xFF, 0xFF));
        chart1.getStyler().setAxisTickLabelsColor(new Color(0xFF, 0, 0));

        BitmapEncoder.saveBitmap(chart1, String.format("./%s_light.jpg", symbol), BitmapEncoder.BitmapFormat.JPG);
        String s3KeyNameLight = String.format("%s_light.jpg", symbol);
        uploadToS3(s3KeyNameLight, String.format("%s_light.jpg", symbol));


    }
}



