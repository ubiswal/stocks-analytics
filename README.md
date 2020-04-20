# Stocks Analytics
This is an analytics daemon that is a part of the full stack of software that aggregates data regarding stock prices and news for a list of companies and aggregates them for viewing on a website. Check the running version [here](http://ec2-3-82-236-182.compute-1.amazonaws.com:8080/).

## Features
The analytics runs through raw files generated by the [crawler](https://github.com/ubiswal/crawlers.git), generates insights that we want to show to the users and uploads them to a database that is read by the [frontend](https://github.com/ubiswal/stocks-web.git).

The list of insights generated is an expanding list. Currently, we generate the following insights:
  - The highest stock price in the last 24 hours (does not include after hours data).
  - The best time to buy/sell, and the best possible profit. 
  - A graph of the stock prices for last 24 hours.
  - Make related news articles viewable by the UI.
  
## Architecture
The daemon an instance of a `java.util.TimerTask` that calls the different analyzers endlessly, every 2 hours. The analyzer relies on a bunch of 3rd party libraries to function:
  - Jackson to de-serialize json.
  - XChart to generate charts.
  - Timer to schedule the daemon.
  - Twitter's util logging to log.
  
## Build and run
To build the daemon, check out this repository and run the following command:
```bash
mvn clean compile assembly:single
```

Run the generated package like this:
```bash
java -jar target/analytics-*-jar-with-dependencies.jar
```


 
