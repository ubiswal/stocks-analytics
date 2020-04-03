package com.ubiswal.config;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

public class Config {
    @Getter
    @Setter
    private String apiKey;// alpha vantage api key
    @Getter
    @Setter
    private List<String> stockSymbols;
    @Getter
    @Setter
    private String newsApiKey; //newsapi api key
    @Getter
    @Setter
    private Map<String, String> stockNewsSearchStrings;
}
