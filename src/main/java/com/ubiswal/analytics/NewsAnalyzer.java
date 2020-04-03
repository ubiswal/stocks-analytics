package com.ubiswal.analytics;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.logging.Logger;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
class Source{
    @Getter
    @Setter
    private String name;
}
@JsonIgnoreProperties(ignoreUnknown = true)
class Article{
    @Getter
    @Setter
    private Source source;
    @Getter
    @Setter
    private String author;
    @Getter
    @Setter
    private String description;
    @Getter
    @Setter
    private String url;
    @Getter
    @Setter
    private String urlToImage;
}

@JsonIgnoreProperties(ignoreUnknown = true)
class ListOfArticles{
    @Getter
    @Setter
    private List<Article> articles;
}

public class NewsAnalyzer extends AbstractAnalyzer {
    private AmazonS3 s3Client;
    private List<String> stockSymbols;
    private String bucketName;
    private AmazonDynamoDB dynamoDBClient;
    private Logger log = Logger.get(this.getClass());

    public NewsAnalyzer(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, List<String> stockSymbols, String bucketName) {
        super(s3Client, bucketName);
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.stockSymbols = stockSymbols;
        this.bucketName = bucketName;
    }


    private ListOfArticles convertS3JsonToClass(String symbol, String currentDate, String hour) throws IOException {
        S3Object s3obj = s3Client.getObject(bucketName, String.format("%s/%s/%s/news.json", currentDate, hour, symbol));
        S3ObjectInputStream inputStream = s3obj.getObjectContent();
        FileUtils.copyInputStreamToFile(inputStream, new File("news.json"));

        //use jackson for json to class conversion
        ObjectMapper mapper = new ObjectMapper();
        // JSON file to Java object
        ListOfArticles stockNewsObj = mapper.readValue(new File("news.json"), ListOfArticles.class);
        return stockNewsObj;
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
                log.info(String.format("Running news analyzer for symbol %s",symbol));
                ListOfArticles stockObj = convertS3JsonToClass(symbol, currentDate, hour);
                int i = 0;
                for (Article entry: stockObj.getArticles().subList(0,9)) {
                    saveNewsArticleToDynamo(i, entry, symbol);
                    i++;
                }
            } catch (IOException | SdkClientException e) {
                e.printStackTrace();
            }
        }
    }

    private  void saveNewsArticleToDynamo(int articleNum, Article article, String symbol){
        PutItemRequest request = new PutItemRequest();
        request.setTableName("Analytics-testing");

        Map<String, AttributeValue> map = new HashMap<>();
        map.put("symb", new AttributeValue().withS(symbol));
        map.put("type", new AttributeValue().withS(String.format("100_newsarticle_%d", articleNum)));
        map.put("url", new AttributeValue().withS(article.getUrl()));
        map.put("url2image", new AttributeValue().withS(article.getUrlToImage()));
        map.put("desc", new AttributeValue().withS(article.getDescription()));
        map.put("src", new AttributeValue().withS(article.getSource().getName()));
        request.setItem(map);
        try {
            PutItemResult result = dynamoDBClient.putItem(request);
            log.info(String.format("Saved the article num %d for stock %s", articleNum, symbol));
        } catch (AmazonServiceException e) {

            log.error(e, String.format("Source = %s \nauthor = %s \ndescription = %s \nurl= %s  \nurlToImg =  %s",article.getSource(), article.getAuthor(), article.getDescription(), article.getUrl(), article.getUrlToImage()));

        }
    }

}


